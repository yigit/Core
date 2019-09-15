package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.launch

class PipelinePersister<Key, Input, Output>(
    private val fetcher: PipelineStore<Key, Input>,
    private val reader: (Key) -> Flow<Output?>,
    private val writer: suspend (Key, Input) -> Unit,
    private val delete: (suspend (Key) -> Unit)? = null
) : PipelineStore<Key, Output> {
    @ExperimentalCoroutinesApi
    @Suppress("UNCHECKED_CAST")
    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return when {
            request.shouldSkipCache(CacheType.DISK) -> fetchSkippingCache(request)
            else -> diskNetworkCombined(
                request = request,
                alwaysTriggerNetwork = request.refresh
            )
        }
    }

    /**
     * skipping cache, just delegate to the fetcher but update disk w/ any new data from fetcher
     */
    private fun fetchSkippingCache(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return fetcher.stream(request)
            .flatMapLatest { response: StoreResponse<Input> ->
                // explicit type is necessary for type resolution
                flow<StoreResponse<Output>> {
                    val data = response.dataOrNull()
                    if (data != null) {
                        // save into database first
                        writer(request.key, response.requireData())
                        // continue database data
                        var first = true
                        val readerFlow: Flow<StoreResponse<Output>> =
                            reader(request.key).mapNotNull {
                                it?.let {
                                    val origin = if (first) {
                                        first = false
                                        response.origin
                                    } else {
                                        ResponseOrigin.Persister
                                    }
                                    StoreResponse.Data(
                                        value = it,
                                        origin = origin
                                    )
                                }
                            }
                        emitAll(readerFlow)
                    } else {
                        emit(response.swapType())
                    }
                }
            }
    }

    /**
     * We want to stream from disk but also want to refresh. If requested or necessary.
     *
     * If [alwaysTriggerNetwork] is set to [false], we want to return from disk but also fetch from
     * the server if there is nothing in disk.
     * To do that, we need to see the first disk value and then decide to fetch or not.
     * in any case, we always return the Flow from reader.
     *
     * How it works:
     * There are 3 main Flows:
     * a) Network flow which starts the network stream. It is guarded by `networkLock`
     * b) Disk flow. It is controller by the `DiskCommand`s.
     * c) Combination of (a) and (b) to dispatch values to the downstream.
     *
     * Initially, disk flow gets started with a DiskCommand.READ_FIRST. This tells it that this
     * is the initial read from the disk. During this flow, if a `null` value arrives, it decides
     * whether to unlock the Network Flow or not.
     *
     * When network flow runs (commanded by disk)
     *  If Data Arrives -> It stops the disk flow first by sending a DiskCommand.STOP, awaits its
     *  termination and then writes to disk and finally restarts the flow via DiskCommand.READ by
     *  also passing the StoreResponse type.
     *
     *  If error or loading arrives, it simply dispatches it to the networkChannel which will be
     *  consumed by the combined flow (c) to send to downstream along w/ the last read disk data.
     */
    private fun diskNetworkCombined(
        request: StoreRequest<Key>,
        alwaysTriggerNetwork: Boolean
    ): Flow<StoreResponse<Output>> = channelFlow<StoreResponse<Output>> {
        // used to control the disk flow so that we can stop/start it.
        val diskCommands = Channel<DiskCommand>(capacity = Channel.RENDEZVOUS)
        val fetcherCommands = Channel<FetcherCommand>(capacity = Channel.RENDEZVOUS)
        launch {
            // trigger first load
            diskCommands.send(DiskCommand.ReadFirst)
            fetcherCommands.consumeAsFlow().collectLatest {
                fetcher.stream(request).collect {
                    val data = it.dataOrNull()
                    if (data != null) {
                        try {
                            // stop disk first
                            val ack = CompletableDeferred<Unit>()
                            diskCommands.send(DiskCommand.Stop(ack))
                            ack.await()
                            writer(request.key, data)
                        } finally {
                            diskCommands.send(DiskCommand.Read(it.origin))
                        }
                    } else {
                        send(it.swapType<Output>())
                    }
                }
            }
        }
        diskCommands.consumeAsFlow().collectLatest { command ->
            when (command) {
                is DiskCommand.Stop -> {
                    command.ack.complete(Unit)
                }
                is DiskCommand.ReadFirst -> {
                    reader(request.key).collectIndexed { index, diskData ->
                        diskData?.let {
                            send(
                                StoreResponse.Data(
                                    value = diskData,
                                    origin = ResponseOrigin.Persister
                                )
                            )
                        }

                        if (index == 0 && (diskData == null || request.refresh)) {
                            fetcherCommands.send(
                                FetcherCommand.Fetch
                            )
                        }
                    }
                }
                is DiskCommand.Read -> {
                    var fetcherOrigin : ResponseOrigin? = command.origin
                    reader(request.key).collect { diskData ->
                        if (diskData != null) {
                            val origin = fetcherOrigin?.let {
                                fetcherOrigin = null
                                it
                            } ?: ResponseOrigin.Persister
                            send(
                                StoreResponse.Data(
                                    value = diskData,
                                    origin = origin
                                )
                            )
                        }
                    }
                }
            }
        }
    }

    override suspend fun clear(key: Key) {
        fetcher.clear(key)
        delete?.invoke(key)
    }

    // used to control the disk flow when combined with network
    internal sealed class DiskCommand {
        object ReadFirst : DiskCommand()
        class Read(val origin: ResponseOrigin) : DiskCommand()
        class Stop(val ack: CompletableDeferred<Unit>) : DiskCommand()
    }

    // used to control the disk flow when combined with network
    internal sealed class FetcherCommand {
        object Fetch : FetcherCommand()
    }
}
