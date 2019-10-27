package com.nytimes.android.external.store4

import com.com.nytimes.suspendCache.StoreCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import com.nytimes.android.external.store3.pipeline.CacheType
import com.nytimes.android.external.store3.pipeline.PipelineStore
import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import com.nytimes.android.external.store3.pipeline.StoreRequest
import com.nytimes.android.external.store3.pipeline.StoreResponse
import com.nytimes.android.external.store3.pipeline.open
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combineTransform
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.flow.withIndex
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger

@ExperimentalCoroutinesApi
@FlowPreview
internal class RealInternalCoroutineStore<Key, Input, Output>(
    private val scope: CoroutineScope,
    private val fetcher: (Key) -> Flow<Input>,
    database: SourceOfTruth<Key, Input, Output>,
    private val memoryPolicy: MemoryPolicy?
) : PipelineStore<Key, Output> {
    private val sourceOfTruth = SourceOfTruthWithBarrier(database)
    private val memCache = StoreCache.fromRequest<Key, Output?, StoreRequest<Key>>(
        loader = {
            TODO(
                """
                    This should've never been called. We don't need this anymore, should remove
                    loader after we clean old Store ?
                """.trimIndent()
            )
        },
        memoryPolicy = memoryPolicy ?: StoreDefaults.memoryPolicy
    )
    private val fetcherController = FetcherController(
        scope = scope,
        fetcher = fetcher,
        sourceOfTruth = sourceOfTruth
    )

    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return diskNetworkCombined3(request)
            .onEach {
                if (it.origin != ResponseOrigin.Cache) {
                    it.dataOrNull()?.let { data ->
                        memCache.put(request.key, data)
                    }
                }
            }
            .onStart {
                if (!request.shouldSkipCache(CacheType.MEMORY)) {
                    memCache.getIfPresent(request.key)?.let { cached ->
                        println("emitting cached $cached")
                        emit(StoreResponse.Data(value = cached, origin = ResponseOrigin.Cache))
                    }
                }
            }
    }

    override suspend fun clear(key: Key) {
        memCache.invalidate(key)
        sourceOfTruth.delete(key)
    }

    private fun diskNetworkCombined3(
        request: StoreRequest<Key>
    ): Flow<StoreResponse<Output>> {
        val diskLock = CompletableDeferred<Unit>()
        val networkLock = CompletableDeferred<Unit>()
        val networkFlow = fetcherController
            .getFetcher(request.key)
            .onEach {
                diskLock.complete(Unit)
            }
            .map {
                SkipOrValue.Value(it) as SkipOrValue<Input>
            }
            .onStart {
                if (!request.shouldSkipCache(CacheType.DISK)) {
                    emit(SkipOrValue.Skip<Input>())
                    println("awaiting network lock")
                    // wait until network gives us the go
                    networkLock.await()
                } else {
                    println("network active immediately")
                }
            }
        val diskFlow = sourceOfTruth.reader(request.key)
            .map {
                SkipOrValue.Value(it) as SkipOrValue<DataWithOrigin<Output>>
            }
            .onStart {
                if (request.shouldSkipCache(CacheType.DISK)) {
                    emit(SkipOrValue.Skip())
                    println("awaiting disk lock")
                    diskLock.await()
                } else {
                    println("disk active immediately")
                }
            }
        // track this to avoid dispatching same local data due to network events
        val lastDispatchedVersion = AtomicInteger(-1)
        return networkFlow.combineTransform(diskFlow.withIndex()) {network, (index, diskData) ->
            println("received $network $index $diskData")
            if (network is SkipOrValue.Value) {
                diskLock.complete(Unit)
            }
            if (diskData is SkipOrValue.Value) {
                if (lastDispatchedVersion.get() < index && diskData.value.value != null) {
                    lastDispatchedVersion.set(index)
                    emit(
                        StoreResponse.Data(
                            value = diskData.value.value,
                            origin = diskData.value.origin
                        )
                    )
                }

                if (networkLock.isActive && (diskData.value.value == null || request.refresh)) {
                    networkLock.complete(Unit)
                }
            }
        }
    }

    private fun diskNetworkCombined2(
        request: StoreRequest<Key>
    ): Flow<StoreResponse<Output>> {
        val managedSubscription = fetcherController.createSubscription(request.key)
        return sourceOfTruth.reader(request.key)
            .onStart {
                if (request.shouldSkipCache(CacheType.DISK)) {
                    managedSubscription.start()
                    // block until server sends a value
                    try {
                        managedSubscription.firstValue.await()
                    } catch (th: Throwable) {
                        println("recieved $th")
                        throw th
                    }
                }
            }
            .onCompletion {
                managedSubscription.stop()
            }
            .withIndex()
            .transform { (index, diskData) ->
                if (diskData.value != null) {
                    emit(
                        StoreResponse.Data(
                            value = diskData.value,
                            origin = diskData.origin
                        )
                    )
                }
                if (index == 0 && (diskData.value == null || request.refresh)) {
                    managedSubscription.start()
                }
            }
    }

    /**
     * We want to stream from disk but also want to refresh. If requested or necessary.
     *
     * To do that, we need to see the first disk value and then decide to fetch or not.
     * in any case, we always return the Flow from reader.
     *
     * How it works:
     * We start by reading the disk. If first response from disk is `null` OR `request.refresh`
     * is set to `true`, we start fetcher flow.
     *
     * When fetcher emits data, if it is [StoreResponse.Error] or [StoreResponse.Loading], it
     * directly goes to the downstream.
     * If it is [StoreResponse.Data], we first stop the disk flow, write the new data to disk and
     * restart the disk flow. On restart, disk-flow sets the first emissions `origin` to the
     * `origin` set by the fetcher. subsequent reads use origin [ResponseOrigin.Persister].
     */
    private fun diskNetworkCombined(
        request: StoreRequest<Key>
    ): Flow<StoreResponse<Output>> = channelFlow {
        // used to control the disk flow so that we can stop/start it.
        val diskCommands = Channel<DiskCommand>(capacity = Channel.CONFLATED)
        // used to control the network flow so that we can decide if we want to start it
        val fetcherCommands = Channel<FetcherCommand>(capacity = Channel.CONFLATED)
        launch {
            // trigger first load if disk is NOT skipped
            if (!request.shouldSkipCache(CacheType.DISK)) {
                diskCommands.send(DiskCommand.Read)
            }

            fetcherCommands.consumeAsFlow().onStart {
                // if skipping cache, start with a network load
                if (request.shouldSkipCache(CacheType.DISK)) {
                    emit(FetcherCommand.Fetch)
                }
            }.collectLatest {
                fetcherController.getFetcher(request.key).collect {
                    // do nothing
                    println("received")
                }
            }
        }
        diskCommands.consumeAsFlow().collect { command ->
            println("disk collect enter")
            try {
                when (command) {
                    is DiskCommand.Read -> {
                        sourceOfTruth.reader(request.key).collectIndexed { index, diskData ->
                            println("disk data: $index $diskData")
                            if (diskData.value != null) {
                                send(
                                    StoreResponse.Data(
                                        value = diskData.value,
                                        origin = diskData.origin
                                    )
                                )
                            }
                            if (index == 0 && (diskData.value == null || request.refresh)) {
                                fetcherCommands.send(
                                    FetcherCommand.Fetch
                                )
                                println("cont1")
                            }
                        }
                    }
                }
            } finally {
                println("disk collect exit")
            }


        }
        awaitClose {
            println("closing")
        }
    }

    // used to control the disk flow when combined with network
    internal sealed class DiskCommand {
        object Read : DiskCommand()
    }

    // used to control the disk flow when combined with network
    internal sealed class FetcherCommand {
        object Fetch : FetcherCommand()
    }

    fun asLegacyStore() = open()

    internal sealed class SkipOrValue<T> {
        class Skip<T>() : SkipOrValue<T>()
        data class Value<T>(val value: T) : SkipOrValue<T>()
    }
}