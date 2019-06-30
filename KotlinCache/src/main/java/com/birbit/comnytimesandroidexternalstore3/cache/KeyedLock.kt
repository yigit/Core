package com.birbit.comnytimesandroidexternalstore3.cache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A read-write lock where you can lock resources by key
 */
internal class KeyedLock<K> {
    private val locks = mutableMapOf<K, Mutex>()
    private val locksMutex = Mutex(false)
    suspend fun <V> withLock(key : K, f : () -> V) : V {
        return try {
            locksMutex.withLock {
                locks.getOrPut(key) {
                    Mutex()
                }
            }.withLock {
                f()
            }
        } finally {
            locksMutex.withLock {
                locks.remove(key)
            }
        }
    }
}