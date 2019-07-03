package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow

@FlowPreview
internal class PipelineFetcherStore<Key, Output>(
        private val fetcher : suspend (Key) -> Flow<Output>
) : PipelineStore<Key, NoInput, Output> {
    override suspend fun stream(key: Key) = fetcher(key)

    override suspend fun streamFresh(key: Key) = fetcher(key)

    override suspend fun clearMemory() {

    }

    override suspend fun clear(key: Key) {
    }
}