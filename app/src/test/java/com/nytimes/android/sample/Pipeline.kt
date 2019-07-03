@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.nytimes.android.sample

import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

interface ReaderMemoryCachePipeline<Key, Output> : ReaderPipeline<Key, Output> {
    suspend fun clearMemory(key: Key)
    suspend fun clearAllMemory()
}

interface ReaderPipeline<Key, Output> {
    suspend fun stream(key: Key): Flow<Output>
}

interface WriterPipeline<Key, Input> {
    suspend fun put(key: Key, input: Input)
}

interface ReadWritePipeline<Key, Input, Output> :
        ReaderPipeline<Key, Output>,
        WriterPipeline<Key, Input>

fun <Key, Output, NewOutput> ReaderPipeline<Key, Output>.withConverter(
        convert: suspend (Output) -> NewOutput
): ReaderPipeline<Key, NewOutput> {
    val delegate = this
    return object : ReaderPipeline<Key, NewOutput> {
        override suspend fun stream(key: Key): Flow<NewOutput> {
            return delegate.stream(key).map {
                convert(it)
            }
        }
    }
}

fun <Key, Output> ReaderPipeline<Key, Output>.withCache(
        memoryPolicy: MemoryPolicy? = null
): ReaderMemoryCachePipeline<Key, Output> {
    return CachePipeline.from(
            reader = this,
            memoryPolicy = memoryPolicy)
}

fun <Key, Output, Output2> ReaderPipeline<Key, Output>.withPersister(
        streamer: suspend (Key) -> Flow<Output2>,
        writer: suspend (Key, Output) -> Unit
): ReadWritePipeline<Key, Output, Output2> {
    val self = this
    return object : ReadWritePipeline<Key, Output, Output2> {
        override suspend fun stream(key: Key): Flow<Output2> {
            return streamer(key)
                    .sideCollect(self.stream(key)) { readerValue: Output ->
                        put(key, readerValue)
                    }
        }

        override suspend fun put(key: Key, input: Output) {
            writer(key, input)
        }
    }
}

object PipelineBuilder {
    fun <Key : Any, Output : Any> getterPipeline(
            getter: suspend (Key) -> Output
    ): ReaderPipeline<Key, Output> {
        return streamerPipeline { key ->
            flow {
                emit(getter(key))
            }
        }
    }

    fun <Key : Any, Output : Any> streamerPipeline(
            streamer: suspend (Key) -> Flow<Output>
    ): ReaderPipeline<Key, Output> {
        return object : ReaderPipeline<Key, Output> {
            override suspend fun stream(key: Key): Flow<Output> {
                return streamer(key)
            }
        }
    }
}

fun <T, R> Flow<T>.sideCollect(
        other: Flow<R>,
        otherCollect: suspend (R) -> Unit) = flow<T> {
    coroutineScope {
        launch {
            other.collect {
                otherCollect(it)
            }
        }
        this@sideCollect.collect {
            emit(it)
        }
    }
}

fun <Key, Output> ReaderPipeline<Key, Output>.asStore(): Store<Output, Key> {
    val self = this
    return object : Store<Output, Key> {
        override suspend fun get(key: Key): Output {
            return self.stream(key).single()
        }

        override suspend fun fresh(key: Key): Output {
            TODO("figure out how to get to the bottom pipe")
        }

        @FlowPreview
        override fun stream(): Flow<Pair<Key, Output>> {
            TODO("not supported")
        }

        @FlowPreview
        override fun stream(key: Key): Flow<Output> {
            return flow {
                self.stream(key).collect {
                    this@flow.emit(it)
                }
            }
        }

        override suspend fun clearMemory() {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override suspend fun clear(key: Key) {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

    }
}