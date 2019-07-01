package com.com.nytimes.suspendCache

import com.nytimes.android.external.store3.base.impl.MemoryPolicy

typealias Loader<K, V> = suspend (K) -> V

interface StoreCache<K, V> {
    suspend fun get(key: K): V
    suspend fun getFresh(key: K): V
    suspend fun put(key: K, value: V)
    suspend fun invalidate(key: K)
    suspend fun clearAll()
    suspend fun getIfPresent(key: K): V?

    companion object {
        fun <K, V> build(
                loader: suspend (K) -> V,
                memoryPolicy: MemoryPolicy
        ): StoreCache<K, V> {
            return StoreCacheImpl(
                    loader = loader,
                    memoryPolicy = memoryPolicy
            )
        }
    }
}