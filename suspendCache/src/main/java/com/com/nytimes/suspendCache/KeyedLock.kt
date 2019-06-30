package com.com.nytimes.suspendCache

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

// TODO: not used in v1, remove before merge
/**
 * A read-write lock where you can lock resources by key
 */
internal class KeyedLock<K> {
    private val locks = mutableMapOf<K, StoreReadWriteLock>()
    private val rwLock = StoreReadWriteLock()

    suspend fun <V> withReadLock(key : K, f : suspend () -> V) : V {
        val keyLock = getKeyLock(key)
        return keyLock.withRead(action = f)
    }

    suspend fun <V> withWriteLock(key : K, f : suspend () -> V) : V {
        val keyLock = getKeyLock(key)
        return keyLock.withWrite(action = f)
    }

    private suspend fun getKeyLock(key : K) : StoreReadWriteLock {
        val lock = rwLock.withRead {
            locks[key]
        }
        return lock ?: rwLock.withWrite {
            // get again, this time w/ write lock
            locks.getOrPut(key) {
                StoreReadWriteLock()
            }
        }
    }
}

// https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock
private class StoreReadWriteLock(
        private val readMutex: Mutex = Mutex(),
        private val writeMutex: Mutex = Mutex()
) {
    @Volatile
    private var readers = 0
    suspend fun <T> withWrite(action : suspend () -> T) : T {
        return writeMutex.withLock {
            action()
        }
    }

    suspend fun <T> withRead(action : suspend () -> T) : T {
        val needsGlobalLock = readMutex.withLock {
            val newReaders = ++ readers
            newReaders == 1
        }
        if (needsGlobalLock) {
            writeMutex.lock()
        }
        try {
            return action()
        } finally {
            val releaseGlobalLock = readMutex.withLock {
                val remainingReaders = --readers
                remainingReaders == 0
            }
            if (releaseGlobalLock) {
                writeMutex.unlock()
            }
        }
    }
}