package com.com.nytimes.suspendCache

import com.nytimes.android.external.cache3.CacheBuilder
import com.nytimes.android.external.cache3.CacheLoader
import com.nytimes.android.external.cache3.Ticker
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.pipeline.StoreRequest

internal class RealPipelineStoreCache<K, V>(
    private val loader: suspend (StoreRequest<K>) -> V,
    private val memoryPolicy: MemoryPolicy,
    ticker: Ticker = Ticker.systemTicker()
) : PipelineCache<K, V> {
    private val realCache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .also {
            if (memoryPolicy.hasAccessPolicy()) {
                it.expireAfterAccess(
                    memoryPolicy.expireAfterAccess,
                    memoryPolicy.expireAfterTimeUnit
                )
            }
            if (memoryPolicy.hasWritePolicy()) {
                it.expireAfterWrite(memoryPolicy.expireAfterWrite, memoryPolicy.expireAfterTimeUnit)
            }
            if (memoryPolicy.hasMaxSize()) {
                it.maximumSize(memoryPolicy.maxSize)
            }
        }
        .build(object : CacheLoader<K, PipelineStoreRecord<K, V>>() {
            override fun load(key: K): PipelineStoreRecord<K, V>? {
                return PipelineStoreRecord(
                    loader = loader
                )
            }
        })

    override suspend fun get(request: StoreRequest<K>): V {
        return realCache.get(request.key)!!.value(request)
    }

    override suspend fun put(request: StoreRequest<K>, value: V) {
        realCache.put(
            request.key, PipelineStoreRecord(
                loader = loader,
                precomputedValue = request to value
            )
        )
    }

    override suspend fun invalidate(key: K) {
        realCache.invalidate(key)
    }

    override suspend fun clearAll() {
        realCache.cleanUp()
    }

    override suspend fun getIfPresent(key: K): Pair<StoreRequest<K>, V>? {
        return realCache.getIfPresent(key)?.cachedValue()
    }
}
