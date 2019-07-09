package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

private fun <Key, In, Out> castConverter(): suspend (Key, In) -> Out {
    return { key, value ->
        @Suppress("UNCHECKED_CAST")
        key as Out
    }
}

@UseExperimental(FlowPreview::class)
class PipelineConverterStore<Key, Input, OldOutput, NewOutput>(
    private val delegate: PipelineStore<Key, Input, OldOutput>,
    private val converter: (suspend (Key, OldOutput) -> NewOutput) = castConverter()
) : PipelineStore<Key, Input, NewOutput> {
    override suspend fun get(request: StoreRequest<Key>): StoreResponse<NewOutput> {
        return delegate.get(request).mapData {
            converter(request.key, it)
        }
    }

    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<NewOutput>> {
        return delegate.stream(request).map {
            it.mapData {
                converter(request.key, it)
            }
        }
    }

    override suspend fun clearMemory() {
        delegate.clearMemory()
    }

    override suspend fun clear(key: Key) {
        delegate.clearMemory()
    }
}