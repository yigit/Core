package com.nytimes.android.external.store3.pipeline

data class StoreRequest<Key> private constructor(
    val key: Key,
    private val skippedCaches: Int
) {
    constructor(key: Key, vararg skippedCaches: CacheType) : this(
        key, skippedCaches.fold(0) { prev, next ->
            prev.or(next.flag)
        }
    )

    fun shouldSkipCache(type: CacheType) = skippedCaches.and(type.flag) != 0

    companion object {
        fun <Key> fresh(key: Key) = StoreRequest(
            key,
            *CacheType.values()
        )

        fun <Key> caced(key: Key) = StoreRequest(
            key,
            0
        )
    }
}

enum class CacheType(internal val flag: Int) {
    MEMORY(0b01),
    DISK(0b10)
}