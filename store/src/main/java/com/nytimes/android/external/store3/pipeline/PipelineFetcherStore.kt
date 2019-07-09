package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single

@FlowPreview
internal class PipelineFetcherStore<Key, Output>(
    private val fetcher: (Key) -> Flow<Output>
) : PipelineStore<Key, NoInput, Output> {
    override suspend fun get(key: Key): StoreResponse<Output> {
        return try {
            StoreResponse.SuccessResponse(fetcher(key).single())
        } catch (ex: Throwable) {
            StoreResponse.ErrorResponse<Output>(
                error = ex,
                data = null
            )
        }
    }

    override suspend fun fresh(key: Key): StoreResponse<Output> {
        return try {
            StoreResponse.SuccessResponse(fetcher(key).single())
        } catch (ex: Throwable) {
            StoreResponse.ErrorResponse<Output>(
                error = ex,
                data = null
            )
        }
    }

    override fun stream(key: Key) = fetcher(key).mapToStoreResponse()

    override fun streamFresh(key: Key) = fetcher(key).mapToStoreResponse()

    override suspend fun clearMemory() {

    }

    override suspend fun clear(key: Key) {
    }
}