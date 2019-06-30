package com.com.nytimes.suspendCache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The value we keep in guava's cache and it handles custom logic for store
 *  * not caching failures
 *  * not having concurrent fetches for the same key
 *  * maybe fresh?
 *  * deduplication
 */
interface StoreRecord<K, V> {
    suspend fun value(): V
    suspend fun cachedValue(): V?
}

internal class StoreRecordPrecomputed<K, V>(
        private val value: V
) : StoreRecord<K, V> {
    override suspend fun cachedValue(): V? = value

    override suspend fun value(): V = value

}

internal class StoreRecordLoader<K, V>(
        private val key: K,
        private val loader: Loader<K, V>
) : StoreRecord<K, V> {
    private var inFlight = Mutex(false)
    @Volatile
    private var _value: V? = null

    override suspend fun cachedValue(): V? = _value

    override suspend fun value(): V {
        val cached = _value
        if (cached != null) {
            return cached
        }
        return inFlight.withLock {
            _value?.let {
                return it
            }
            runCatching {
                loader(key)
            }
        }.also {
            it.getOrNull()?.let {
                _value = it
            }
        }.getOrThrow()
    }
}
