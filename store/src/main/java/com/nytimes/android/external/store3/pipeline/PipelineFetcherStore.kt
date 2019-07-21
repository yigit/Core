package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.drop
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

@FlowPreview
internal class PipelineFetcherStore<Key, Output>(
    private val fetcher: (Key) -> Flow<Output>
) : PipelineStore<Key, Output> {

    override suspend fun get(request: StoreRequest<Key>): Output? {
        return fetcher(request.key).singleOrNull()
    }

    override fun stream(request: StoreRequest<Key>) = fetcher(request.key)

    override suspend fun clearMemory() {

    }

    override suspend fun clear(key: Key) {
    }
}

@FlowPreview
internal class PipelineFetcherStore2<Key, Output>(
        private val fetcher: suspend (Key) -> Output
) : PipelineStore<Key, Output> {
    private val keyValueTracker = KeyValueTracker<Key, Output>()
    override suspend fun get(request: StoreRequest<Key>): Output? {
        return fetcher(request.key).also {
            keyValueTracker.invalidate(request.key, it)
        }
    }

    override fun stream(request: StoreRequest<Key>) = flow<Output> {
        get(request)?.let {
            emit(it)
        }
        keyValueTracker.keyFlow(request.key).collect {
            emit(it)
        }
    }

    override suspend fun clearMemory() {

    }

    override suspend fun clear(key: Key) {
    }
}

@FlowPreview
@ExperimentalCoroutinesApi
private class KeyValueTracker<Key, Value> {
    private val mutex = Mutex()
    private val invalidations = BroadcastChannel<Pair<Key, Value>?>(Channel.CONFLATED).apply {
        //a conflated channel always maintains the last element, the stream method ignore this element.
        //Here we add an empty element that will be ignored later
        offer(null)
    }

    suspend fun invalidate(key: Key, value : Value) = mutex.withLock {
        invalidations.send(key to value)
    }

    @ObsoleteCoroutinesApi
    suspend fun keyFlow(key: Key): Flow<Value> = flow {
        invalidations.openSubscription().drop(1).consumeEach {
            if (it != null && it.first == key) {
                emit(it.second)
            }
        }
    }
}