package com.com.nytimes.suspendCache

import com.nytimes.android.external.store3.pipeline.CacheType
import com.nytimes.android.external.store3.pipeline.StoreRequest
import com.sun.org.apache.xpath.internal.operations.Bool
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The value we keep in guava's cache and it handles custom logic for store
 *  * not caching failures
 *  * not having concurrent fetches for the same key
 *  * maybe fresh?
 *  * deduplication
 */
internal class PipelineStoreRecord<K, V>(
    precomputedValue: Pair<StoreRequest<K>, V>? = null,
    private val loader: Loader<StoreRequest<K>, V>
) {
    private var inFlight = Mutex(false)
    @Volatile
    private var _value: Pair<StoreRequest<K>, V>? = precomputedValue

    fun cachedValue() = _value


    private inline suspend fun internalDoLoadAndCache(request: StoreRequest<K>): V {
        return runCatching {
            loader(request)
        }.also {
            it.getOrNull()?.let {
                _value = request to it
            }
        }.getOrThrow()
    }

    suspend fun value(request: StoreRequest<K>): V {
        val cached = _value
        if (cached != null && cached.first.covers(request)) {
            return cached.second
        }
        return inFlight.withLock {
            val existing = _value
            if (existing != null && existing.first.covers(request)) {
                existing.second
            } else {
                internalDoLoadAndCache(request)
            }
        }
    }

    private fun StoreRequest<K>.covers(other: StoreRequest<K>) : Boolean {
        // if other wants to skip a cache, we cannot use it because we don't know where the data
        // came from.
        return !other.shouldSkipCache(CacheType.DISK) && !other.shouldSkipCache(CacheType.MEMORY)
    }
}
