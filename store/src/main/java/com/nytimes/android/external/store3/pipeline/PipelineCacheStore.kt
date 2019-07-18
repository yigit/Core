package com.nytimes.android.external.store3.pipeline

import com.com.nytimes.suspendCache.PipelineCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach

@FlowPreview
internal class PipelineCacheStore<Key, Output>(
    private val delegate: PipelineStore<Key, Output>,
    memoryPolicy: MemoryPolicy? = null
) : PipelineStore<Key, Output> {
    private val memCache = PipelineCache.from(
        loader = { request: StoreRequest<Key> ->
            delegate.get(request)
        },
        memoryPolicy = memoryPolicy ?: StoreDefaults.memoryPolicy
    )

    override fun stream(request: StoreRequest<Key>): Flow<Output> {
        @Suppress("RemoveExplicitTypeArguments")
        return flow<Output> {
            val cached = memCache.getIfPresent(request.key)
            if (cached?.first?.covers(request) == true) {
                cached.second?.let {
                    emit(it)
                }
            }
            delegate.stream(request).collect {
                memCache.put(request, it)
                emit(it)
            }
        }
    }

    override suspend fun get(request: StoreRequest<Key>): Output? {
        return memCache.get(request)
    }

    override suspend fun clearMemory() {
        memCache.clearAll()
        delegate.clearMemory()
    }

    override suspend fun clear(key: Key) {
        memCache.invalidate(key)
        delegate.clear(key)
    }
}