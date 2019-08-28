package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.map

internal class PipelineFetcherStore<Key, Output>(
    private val fetcher: (Key) -> Flow<Output>
) : PipelineStore<Key, Output> {
    override fun stream(request: StoreRequest<Key>) : Flow<StoreResponse<Output>> = fetcher(request.key)
        .map {
            StoreResponse.Success(it)
        }.catch<StoreResponse<Output>> {
            emit(StoreResponse.Error<Output>(it))
        }

    override suspend fun clear(key: Key) {
    }
}