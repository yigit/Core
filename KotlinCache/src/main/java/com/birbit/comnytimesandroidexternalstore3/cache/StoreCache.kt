package com.birbit.comnytimesandroidexternalstore3.cache

typealias Loader<K, V> = suspend (K) -> V

interface StoreCache<K, V> {
    suspend fun get(key : K, loader : Loader<K, V>) : V
    suspend fun put(key : K, value : V)
    suspend fun invalidate(key : K)
    suspend fun clearAll()
    suspend fun getIfPresent(key : K) : V?
}