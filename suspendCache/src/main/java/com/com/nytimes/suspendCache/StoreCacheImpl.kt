package com.com.nytimes.suspendCache

import com.nytimes.android.external.cache3.CacheBuilder
import com.nytimes.android.external.cache3.CacheLoader
import com.nytimes.android.external.cache3.Ticker
import com.nytimes.android.external.store3.base.impl.MemoryPolicy

internal class StoreCacheImpl<K, V>(
        private val loader: suspend (K) -> V,
        private val memoryPolicy: MemoryPolicy,
        ticker: Ticker = Ticker.systemTicker()
) : StoreCache<K, V> {
    private val realCache = CacheBuilder.newBuilder()
            .ticker(ticker)
            .also {
                if (memoryPolicy.hasAccessPolicy()) {
                    it.expireAfterAccess(memoryPolicy.expireAfterAccess, memoryPolicy.expireAfterTimeUnit)
                }
                if (memoryPolicy.hasWritePolicy()) {
                    it.expireAfterWrite(memoryPolicy.expireAfterWrite, memoryPolicy.expireAfterTimeUnit)
                }
                if (memoryPolicy.hasMaxSize()) {
                    it.maximumSize(memoryPolicy.maxSize)
                }
            }
            .build(object : CacheLoader<K, StoreRecord<K, V>>() {
                override fun load(key: K): StoreRecord<K, V>? {
                    return StoreRecordLoader(key, loader)
                }

            })

    override suspend fun get(key: K): V {
        return realCache.get(key) {
            @Suppress("UNCHECKED_CAST")
            (StoreRecordLoader(key, loader))
        }.value()
    }

    override suspend fun put(key: K, value: V) {
        realCache.put(key, StoreRecordPrecomputed(value))
    }

    override suspend fun invalidate(key: K) {
        realCache.invalidate(key)
    }

    override suspend fun clearAll() {
        realCache.cleanUp()
    }

    override suspend fun getIfPresent(key: K): V? {
        return realCache.getIfPresent(key)?.cachedValue()
    }

}