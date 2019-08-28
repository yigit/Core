package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.mapNotNull

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
            // skipping cache, just delegate to the fetcher but update disk w/ any new data from
            // fetcher
            request.shouldSkipCache(CacheType.DISK) -> fetcher.stream(request)
                .flatMapLatest { response: StoreResponse<Input> ->
                    writer(request.key, response.requireData())
                    // TODO handle loading
                    reader(request.key).mapNotNull {
                        it?.let {
                            response.swapData(it)
                        }
                    }
                }
            // we want to stream from disk but also want to refresh. Immediately start fetcher
            // when flow starts.
            request.refresh -> reader(request.key)
                .sideCollect(fetcher.stream(request)) { response: StoreResponse<Input> ->
                    response.dataOrNull()?.let { data: Input ->
                        writer(request.key, data)
                    }
                }.mapNotNull {
                    // TODO handle loading
                    it?.let {
                        StoreResponse.Success(it)
                    }
                }
            // we want to return from disk but also fetch from server if there is nothing in disk.
            // to do that, we need to see the first disk value and then decide to fetch or not.
            // in any case, we always return the Flow from reader.
            else -> reader(request.key)
                .sideCollectMaybe(
                    otherProducer = {
                        if (it == null) {
                            // disk returned null, create a new flow from fetcher
                            fetcher.stream(request)
                        } else {
                            // disk had cached value, don't trigger fetcher
                            null
                        }
                    },
                    otherCollect = { response: StoreResponse<Input> ->
                        response.dataOrNull()?.let { data: Input ->
                            writer(request.key, data)
                        }
                    }
                ).mapNotNull {
                    it?.let {
                        // TODO handle loading
                        StoreResponse.Success(it)
                    }
                }
        }
    }


    override suspend fun clear(key: Key) {
        fetcher.clear(key)
        delete?.invoke(key)
    }
}
