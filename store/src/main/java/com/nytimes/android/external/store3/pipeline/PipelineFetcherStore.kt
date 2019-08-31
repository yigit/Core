package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

internal class PipelineFetcherStore<Key, Output>(
    private val fetcher: (Key) -> Flow<Output>
) : PipelineStore<Key, Output> {
    @ExperimentalCoroutinesApi
    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return flow {
            // first emit loading, then emit all values
            emit(StoreResponse.Loading())
            val fetcherFlow = fetcher(request.key)
                .map {
                    StoreResponse.Success(it)
                }.catch<StoreResponse<Output>> {
                    emit(StoreResponse.Error(it))
                }
            emitAll(fetcherFlow)
        }
    }
    override suspend fun clear(key: Key) {
    }
}