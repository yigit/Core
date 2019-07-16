package com.nytimes.android.external.store3.pipeline

import com.com.nytimes.suspendCache.StoreCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach

@FlowPreview
internal class PipelineCacheStore<Key, Input, Output>(
    private val delegate: PipelineStore<Key, Input, Output>,
    memoryPolicy: MemoryPolicy? = null
) : PipelineStore<Key, Input, Output> {
    private val memCache = StoreCache.from(
        loader = { key: Key ->
            // TODO StoreRequest needs to be something different since cache rules might be
            // different in separate calls
            delegate.get(StoreRequest.cached(key, refresh = false)).throwIfError()
        },
        memoryPolicy = memoryPolicy ?: StoreDefaults.memoryPolicy
    )

    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        if (request.shouldSkipCache(CacheType.MEMORY)) {
            return delegate.stream(request)
                .onEach {
                    memCache.put(request.key, it)
                }
        } else {
            @Suppress("RemoveExplicitTypeArguments")
            return flow<StoreResponse<Output>> {
                val cached = memCache.getIfPresent(request.key)?.dataOrNull()
                emit(
                    StoreResponse.LoadingResponse(
                        data = cached
                    )
                )
                delegate.stream(request).collect {
                    memCache.put(request.key, it)
                    emit(it)
                }
            }
        }
    }

    override suspend fun get(request: StoreRequest<Key>): StoreResponse<Output> {
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