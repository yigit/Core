package com.com.nytimes.suspendCache

import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.pipeline.StoreRequest

typealias Loader<K, V> = suspend (K) -> V

/**
 * Cache definition used by Store internally.
 */
interface PipelineCache<K, V> {
    suspend fun get(request: StoreRequest<K>): V
    suspend fun put(request: StoreRequest<K>, value: V)
    suspend fun invalidate(key: K)
    suspend fun clearAll()
    suspend fun getIfPresent(key: K): V?

    companion object {
        fun <K, V> from(
            loader: suspend (StoreRequest<K>) -> V,
            memoryPolicy: MemoryPolicy
        ): PipelineCache<K, V> {
            return RealPipelineStoreCache(
                loader = loader,
                memoryPolicy = memoryPolicy
            )
        }
    }
}
