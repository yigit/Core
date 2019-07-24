package com.com.nytimes.suspendCache

import com.nytimes.android.external.store3.pipeline.CacheType
import com.nytimes.android.external.store3.pipeline.StoreRequest
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
        precomputedValue: V? = null,
        private val loader: Loader<StoreRequest<K>, V>
) {
    private var inFlight = Mutex(false)
    @Volatile
    private var _value = precomputedValue

    fun cachedValue() = _value


    private suspend inline fun internalDoLoadAndCache(request: StoreRequest<K>): V {
        return runCatching {
            loader(request)
        }.also {
            it.getOrNull()?.let {
                _value = it
            }
        }.getOrThrow()
    }

    suspend fun value(request: StoreRequest<K>): V {
        if (!request.shouldSkipCache(CacheType.MEMORY)) {
            _value?.let {
                return it
            }
        }
        return inFlight.withLock {
            if (!request.shouldSkipCache(CacheType.MEMORY)) {
                _value?.let {
                    return it
                }
            }
            internalDoLoadAndCache(request)
        }
    }
}
