package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

private fun <Key, In, Out> castConverter(): suspend (Key, In) -> Out {
    return { key, value ->
        @Suppress("UNCHECKED_CAST")
        key as Out
    }
}

class PipelineConverterStore<Key, OldOutput, NewOutput>(
    private val delegate: PipelineStore<Key, OldOutput>,
    private val converter: (suspend (Key, OldOutput) -> NewOutput) = castConverter()
) : PipelineStore<Key, NewOutput> {

    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<NewOutput>> {
        return delegate.stream(request)
            .map {
                val data = it.dataOrNull()
                if (data == null) {
                    it.swapData<NewOutput>(null)
                } else {
                    it.swapData(converter(request.key, data))
                }
            }
    }

    override suspend fun clear(key: Key) {
        delegate.clear(key)
    }
}