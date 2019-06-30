package com.com.nytimes.suspendCache

typealias Loader<K, V> = suspend (K) -> V

interface StoreCache<K, V> {
    suspend fun get(key : K) : V
    suspend fun put(key : K, value : V)
    suspend fun invalidate(key : K)
    suspend fun clearAll()
    suspend fun getIfPresent(key : K) : V?
}