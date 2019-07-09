package com.nytimes.android.external.store3.pipeline

import com.com.nytimes.suspendCache.StoreCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

@FlowPreview
internal class PipelineCacheStore<Key, Output>(
        private val delegate: PipelineStore<Key, Output>,
        memoryPolicy: MemoryPolicy? = null
) : PipelineStore<Key, Output> {
    private val memCache = StoreCache.from(
        loader = { key: Key ->
            // TODO StoreRequest needs to be something different since cache rules might be
            // different in separate calls
            delegate.get(StoreRequest.caced(key))
        },
        memoryPolicy = memoryPolicy ?: StoreDefaults.memoryPolicy
    )

    override fun stream(request: StoreRequest<Key>): Flow<Output> {
        if (request.shouldSkipCache(CacheType.MEMORY)) {
            return delegate.stream(request)
                .onEach {
                    memCache.put(request.key, it)
                }
        } else {
            TODO("need info about where we want to refresh from")
        }
    }

    override suspend fun get(request: StoreRequest<Key>): Output? {
        if (request.shouldSkipCache(CacheType.MEMORY)) {
            return memCache.fresh(key = request.key)
        } else {
            return memCache.get(key = request.key)
        }
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