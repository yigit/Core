package com.nytimes.android.external.store3.pipeline

import com.com.nytimes.suspendCache.StoreCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

internal class PipelineCacheStore<Key, Output>(
    private val delegate: PipelineStore<Key, Output>,
    memoryPolicy: MemoryPolicy? = null
) : PipelineStore<Key, Output> {
    private val memCache = StoreCache.fromRequest<Key, Output?, StoreRequest<Key>>(
        loader = {
            TODO(
                """
                    This should've never been called. We don't need this anymore, should remove
                    loader after we clean old Store ?
                """.trimIndent()
            )
        },
        memoryPolicy = memoryPolicy ?: StoreDefaults.memoryPolicy
    )

    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        @Suppress("RemoveExplicitTypeArguments")
        return flow<StoreResponse<Output>> {
            var dispatchedLoading = false
            if (!request.shouldSkipCache(CacheType.MEMORY)) {
                val cached = memCache.getIfPresent(request.key)
                cached?.let {
                    dispatchedLoading = true
                    if (request.refresh) {
                        emit(StoreResponse.Loading(it))
                    } else {
                        emit(StoreResponse.Success(it))
                        throw AbortFlowException()
                    }
                }
            }

            delegate.stream(request).collect {
                // save into cache if it has data
                it.dataOrNull()?.let {
                    memCache.put(request.key, it)
                }
                // dispatch it if it is not loading or we've not dispatched loading
                if (!dispatchedLoading || it !is StoreResponse.Loading) {
                    emit(it)
                }
            }
        }
    }

    override suspend fun clear(key: Key) {
        memCache.invalidate(key)
        delegate.clear(key)
    }
}