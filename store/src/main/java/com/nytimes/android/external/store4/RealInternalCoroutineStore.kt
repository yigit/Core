package com.nytimes.android.external.store4

import com.com.nytimes.suspendCache.StoreCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import com.nytimes.android.external.store3.pipeline.CacheType
import com.nytimes.android.external.store3.pipeline.PipelineStore
import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import com.nytimes.android.external.store3.pipeline.SimplePersisterAsFlowable
import com.nytimes.android.external.store3.pipeline.StoreRequest
import com.nytimes.android.external.store3.pipeline.StoreResponse
import com.nytimes.android.external.store3.pipeline.open
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.flow.withIndex

@ExperimentalCoroutinesApi
@FlowPreview
internal class RealInternalCoroutineStore<Key, Input, Output>(
    private val scope: CoroutineScope,
    private val fetcher: (Key) -> Flow<Input>,
    database: SourceOfTruth<Key, Input, Output>,
    private val memoryPolicy: MemoryPolicy?
) : PipelineStore<Key, Output> {
    private val sourceOfTruth = SourceOfTruthWithBarrier(database)
    private val memCache = memoryPolicy?.let {
        StoreCache.fromRequest<Key, Output?, StoreRequest<Key>>(
            loader = {
                TODO(
                    """
                    This should've never been called. We don't need this anymore, should remove
                    loader after we clean old Store ?
                """.trimIndent()
                )
            },
            memoryPolicy = memoryPolicy
        )
    }
    private val fetcherController = FetcherController(
        scope = scope,
        realFetcher = fetcher,
        sourceOfTruth = sourceOfTruth
    )

    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return diskNetworkCombined(request)
            .onEach {
                if (it.origin != ResponseOrigin.Cache) {
                    it.dataOrNull()?.let { data ->
                        memCache?.put(request.key, data)
                    }
                }
            }
            .onStart {
                if (!request.shouldSkipCache(CacheType.MEMORY)) {
                    memCache?.getIfPresent(request.key)?.let { cached ->
                        emit(StoreResponse.Data(value = cached, origin = ResponseOrigin.Cache))
                    }
                }
            }
    }

    override suspend fun clear(key: Key) {
        memCache?.invalidate(key)
        sourceOfTruth.delete(key)
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
    ): Flow<StoreResponse<Output>> {
        val diskLock = CompletableDeferred<Unit>()
        val networkLock = CompletableDeferred<Unit>()
        val networkFlow = fetcherController
            .getFetcher(request.key)
            .map {
                StoreResponse.Data(
                    value = it,
                    origin = ResponseOrigin.Fetcher
                ) as StoreResponse<Input>
            }.catch {
                emit(
                    StoreResponse.Error(
                        error = it,
                        origin = ResponseOrigin.Fetcher
                    )
                )
            }
            .onStart {
                if (!request.shouldSkipCache(CacheType.DISK)) {
                    // wait until network gives us the go
                    networkLock.await()
                }
                emit(
                    StoreResponse.Loading(
                        origin = ResponseOrigin.Fetcher
                    )
                )
            }
        if (!request.shouldSkipCache(CacheType.DISK)) {
            diskLock.complete(Unit)
        }
        val diskFlow = sourceOfTruth.reader(request.key, diskLock)
        // track this to avoid dispatching same local data due to network events
        return networkFlow.merge(diskFlow.withIndex())
            .onStart {
                // TODO consalidate these acquire/releases we don't need this many everywhere
                //  probably just here is enough

                // TODO this acquire might hold the value but inadvertanly make us think that a
                //  fetcher value is a disk value, even though it was fetched after we've subscribed
                //  because inner source does not know that we've subscribed earlier so we'll still
                //  be reading it as if it was fetched before us.
                //  by having a time value here, we can let it evaluate to proper type even though
                //  we are or we are not fetching the value
            }.onCompletion {

            }
            .transform {
            if (it is Either.Left) {
                if (it.value !is StoreResponse.Data<*>) {
                    emit(it.value.swapType())
                }
                // network sent something
                if (it.value is StoreResponse.Data<*>) {
                    // unlocking disk only if network sent data so that fresh data request never
                    // receives disk data by mistake
                    diskLock.complete(Unit)
                }

            } else if (it is Either.Right) {
                // right, that is data from disk
                val (index, diskData) = it.value
                if (diskData.value != null) {
                    emit(
                        StoreResponse.Data(
                            value = diskData.value,
                            origin = diskData.origin
                        ) as StoreResponse<Output>
                    )
                }

                if (index == 0 && (diskData.value == null || request.refresh)) {
                    networkLock.complete(Unit)
                }
            }
        }.onEach {
                println("emitting $it")
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

    // TODO this builder w/ 3 type args is really ugly, think more about it...
    companion object {
        fun <Key, Input, Output> beginWithNonFlowingFetcher(
            fetcher: suspend (key: Key) -> Input
        ) = Builder<Key, Input, Output>().nonFlowingFetcher(fetcher)

        fun <Key, Input, Output> beginWithFlowingFetcher(
            fetcher: (key: Key) -> Flow<Input>
        ) = Builder<Key, Input, Output>().fetcher(fetcher)
    }

    class Builder<Key, Input, Output> {
        private lateinit var _fetcher: (key: Key) -> Flow<Input>
        private var _scope: CoroutineScope? = null
        private var _sourceOfTruth: SourceOfTruth<Key, Input, Output>? = null
        private var _cachePolicy: MemoryPolicy? = StoreDefaults.memoryPolicy
        fun nonFlowingFetcher(
            fetcher: suspend (key: Key) -> Input
        ): Builder<Key, Input, Output> {
            _fetcher = { key: Key ->
                flow {
                    emit(fetcher(key))
                }
            }
            return this
        }

        fun fetcher(
            fetcher: (key: Key) -> Flow<Input>
        ): Builder<Key, Input, Output> {
            _fetcher = fetcher
            return this
        }

        fun scope(scope: CoroutineScope): Builder<Key, Input, Output> {
            _scope = scope
            return this
        }

        fun nonFlowingPersister(
            reader: suspend (Key) -> Output?,
            writer: suspend (Key, Input) -> Unit,
            delete: (suspend (Key) -> Unit)? = null
        ): Builder<Key, Input, Output> {
            val flowingPersister = SimplePersisterAsFlowable(
                reader = reader,
                writer = writer,
                delete = delete
            )
            return persister(
                reader = flowingPersister::flowReader,
                writer = flowingPersister::flowWriter,
                delete = flowingPersister::flowDelete
            )
        }

        fun persister(
            reader: (Key) -> Flow<Output?>,
            writer: suspend (Key, Input) -> Unit,
            delete: (suspend (Key) -> Unit)? = null
        ): Builder<Key, Input, Output> {
            _sourceOfTruth = PersistentSourceOfTruth(
                realReader = reader,
                realWriter = writer,
                realDelete = delete
            )
            return this
        }

        fun sourceOfTruth(
            sourceOfTruth: SourceOfTruth<Key, Input, Output>
        ): Builder<Key, Input, Output> {
            _sourceOfTruth = sourceOfTruth
            return this
        }

        fun cachePolicy(memoryPolicy: MemoryPolicy?): Builder<Key, Input, Output> {
            _cachePolicy = memoryPolicy
            return this
        }

        fun disableCache(): Builder<Key, Input, Output> {
            _cachePolicy = null
            return this
        }

        fun build(): RealInternalCoroutineStore<Key, Input, Output> {
            check(::_fetcher.isInitialized) {
                "must provide a fetcher"
            }
            @Suppress("UNCHECKED_CAST")
            return RealInternalCoroutineStore<Key, Input, Output>(
                scope = _scope ?: GlobalScope,
                database = _sourceOfTruth
                    ?: InMemorySourceOfTruth<Key, Output>() as SourceOfTruth<Key, Input, Output>,
                fetcher = _fetcher,
                memoryPolicy = _cachePolicy
            )
        }
    }
}