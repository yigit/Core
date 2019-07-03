package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

private fun <In, Out> castConverter() : suspend (In) -> Out {
    return {
        @Suppress("UNCHECKED_CAST")
        it as Out
    }
}

@UseExperimental(FlowPreview::class)
class PipelineConverterStore<Key, Input, OldOutput, NewOutput>(
        private val delegate : PipelineStore<Key, Input, OldOutput>,
        private val converter : (suspend(OldOutput) -> NewOutput) = castConverter()
) : PipelineStore<Key, Input, NewOutput> {
    override suspend fun stream(key: Key): Flow<NewOutput> {
        return delegate.stream(key).map(converter)
    }

    override suspend fun streamFresh(key: Key): Flow<NewOutput> {
        return delegate.streamFresh(key).map(converter)
    }

    override suspend fun clearMemory() {
        delegate.clearMemory()
    }

    override suspend fun clear(key: Key) {
        delegate.clearMemory()
    }
}