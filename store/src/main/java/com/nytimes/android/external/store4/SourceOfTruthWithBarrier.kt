package com.nytimes.android.external.store4

import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@FlowPreview
@ExperimentalCoroutinesApi
internal class SourceOfTruthWithBarrier<Key, Input, Output>(
    private val delegate: SourceOfTruth<Key, Input, Output>
) : SourceOfTruth<Key, Input, DataWithOrigin<Output>> {
    override suspend fun acquire(key: Key) {
        delegate.acquire(key)
    }

    override suspend fun release(key: Key) {
        delegate.release(key)
    }

    override val defaultOrigin: ResponseOrigin = delegate.defaultOrigin
    // TODO need to clean these up
    private val barriers = mutableMapOf<Key, ConflatedBroadcastChannel<BarrierMsg>>()
    private val barrierLock = ReentrantLock()

    private fun getBarrier(key: Key) = barrierLock.withLock {
        barriers.getOrPut(key) {
            ConflatedBroadcastChannel(BarrierMsg.Initial.INSTANCE)
        }
    }

    override fun reader(key: Key): Flow<DataWithOrigin<Output>> {
        return getBarrier(key).asFlow()
            .onStart {
                acquire(key)
            }
            .onCompletion {
                reader(key)
            }.flatMapLatest {
                when (it) {
                    is BarrierMsg.Initial -> delegate.reader(key).map {
                        DataWithOrigin<Output>(origin = defaultOrigin, value = it)
                    }
                    is BarrierMsg.Open -> delegate.reader(key).mapIndexed { index, output ->
                        DataWithOrigin<Output>(origin = defaultOrigin, value = output)
                    }
                    is BarrierMsg.Blocked -> {
                        it.ack.complete(Unit)
                        flowOf()
                    }
                }
            }
    }

    override suspend fun write(key: Key, value: Input) {
        // TODO multiple downstream is still broken!
        //  this acking will work only for one
        //  also, it will get stuck if downstream closes but then we would close as well
        val ack = CompletableDeferred<Unit>()
        getBarrier(key).send(BarrierMsg.Blocked(ack))
        // TODO do we need ackiing still since we are using a barrier anyways ?
        //  it is a problem if downstream is suspended since it cannot reply the ack :/
//        ack.await()
        delegate.write(key, value)
        getBarrier(key).send(BarrierMsg.Open.INSTANCE)
    }

    override suspend fun delete(key: Key) {
        delegate.delete(key)
    }

    override suspend fun getSize(): Int {
        return delegate.getSize()
    }

    private sealed class BarrierMsg {
        class Blocked(val ack: CompletableDeferred<Unit>) : BarrierMsg()
        class Open private constructor() : BarrierMsg() {
            companion object {
                val INSTANCE = Open()
            }
        }

        class Initial private constructor() : BarrierMsg() {
            companion object {
                val INSTANCE = Initial()
            }
        }
    }
}

internal data class DataWithOrigin<T>(
    val origin: ResponseOrigin,
    val value: T?
)

private inline fun <T, R> Flow<T>.mapIndexed(crossinline block: (Int, T) -> R) = flow {
    this@mapIndexed.collectIndexed { index, value ->
        emit(block(index, value))
    }
}