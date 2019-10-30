package com.nytimes.android.external.store4

import com.nytimes.android.external.store3.base.Clearable
import com.nytimes.android.external.store3.base.Persister
import com.nytimes.android.external.store3.pipeline.KeyTracker
import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import com.nytimes.android.external.store3.util.KeyParser
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal interface SourceOfTruth<Key, Input, Output> {
    val defaultOrigin: ResponseOrigin
    fun reader(key: Key): Flow<Output?>
    suspend fun write(key: Key, value: Input)
    suspend fun delete(key: Key)
    // for testing
    suspend fun getSize(): Int

    suspend fun acquire(key : Key)
    suspend fun release(key : Key)

    companion object {
        fun <Key, Output> fromLegacy(
            persister: Persister<Output, Key>? = null,
            // parser that runs after get from db
            postParser: KeyParser<Key, Output, Output>? = null
        ): SourceOfTruth<Key, Output, Output> {
            if (persister == null) {
                return InMemorySourceOfTruth()
            } else {
                return PersistentSourceOfTruth(
                    realReader = { key ->
                        flow {
                            if (postParser == null) {
                                emit(persister.read(key))
                            } else {
                                persister.read(key)?.let {
                                    println("received $it from persister")
                                    val postParsed = postParser.apply(key, it)
                                    emit(postParsed)
                                } ?: emit(null)
                            }
                        }
                    },
                    realWriter = { key, value ->
                        persister.write(key, value)
                    },
                    realDelete = { key ->
                        (persister as? Clearable<Key>)?.clear(key)
                    }
                )
            }
        }
    }
}

internal class PersistentSourceOfTruth<Key, Input, Output>(
    private val realReader: (Key) -> Flow<Output?>,
    private val realWriter: suspend (Key, Input) -> Unit,
    private val realDelete: (suspend (Key) -> Unit)? = null
) : SourceOfTruth<Key, Input, Output> {
    override suspend fun acquire(key: Key) {
        // ignore
    }

    override suspend fun release(key: Key) {
        // ignore
    }

    override val defaultOrigin = ResponseOrigin.Persister
    override fun reader(key: Key): Flow<Output?> = realReader(key)
    override suspend fun write(key: Key, value: Input) = realWriter(key, value)
    override suspend fun delete(key: Key) {
        realDelete?.invoke(key)
    }

    // for testing
    override suspend fun getSize(): Int {
        throw UnsupportedOperationException("not supported for persistent")
    }
}

/**
 * A source of truth implementation that keeps data in memory as long as there is an active
 * flow consuming that key. Otherwise, it clears it.
 */
@ExperimentalCoroutinesApi
internal class InMemorySourceOfTruth<Key, Output>
    : SourceOfTruth<Key, Output, Output> {
    override suspend fun acquire(key: Key) {
        lock.withLock {
            getOrCreateEntry(key).listeners ++
        }
    }

    override suspend fun release(key: Key) {
        lock.withLock {
            val entry = checkNotNull(data[key]) {
                "on completion of flow, we should've had an entry"
            }
            entry.listeners--
            if (entry.listeners == 0) {
                println("no one is observing $key, delete")
                data.remove(key)
            }
        }
    }

    override val defaultOrigin = ResponseOrigin.Fetcher
    private val data = mutableMapOf<Key, Entry<Output>>()
    private val lock = Mutex()
    private val keyTracker = KeyTracker<Key>()

    override fun reader(key: Key): Flow<Output?> {
        return keyTracker.keyFlow(key).onStart {
            println("key flow start $key")
            acquire(key)
        }.catch {
            println("error hallened $it")
            throw it
        }.onCompletion {
                println("key flow end $key")
            release(key)
        }.map {
            lock.withLock {
                data[key]?.value
            }
        }
    }

    override suspend fun write(key: Key, value: Output) {
        println("writing data")
        lock.withLock {
            // only set if there is an observer
            data[key]?.value = value
        }
        keyTracker.invalidate(key)
    }

    override suspend fun delete(key: Key) {
        lock.withLock {
            data[key]?.value = null
        }
        keyTracker.invalidate(key)
    }

    private fun getOrCreateEntry(key: Key): Entry<Output> {
        return data.getOrPut(key) {
            Entry(
                listeners = 0,
                value = null
            )
        }
    }

    override suspend fun getSize() = lock.withLock {
        data.size
    }

    private data class Entry<Output>(
        var listeners: Int,
        var value: Output?
    )
}
