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
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@FlowPreview
@ExperimentalCoroutinesApi
internal class SourceOfTruthWithBarrier<Key, Input, Output>(
    private val delegate: SourceOfTruth<Key, Input, Output>
) {
    // TODO need to clean these up
    private val barriers = mutableMapOf<Key, ConflatedBroadcastChannel<BarrierMsg>>()
    private val barrierLock = ReentrantLock()
    private val versionCounter = AtomicLong(0)

    private fun getBarrier(key: Key) = barrierLock.withLock {
        barriers.getOrPut(key) {
            ConflatedBroadcastChannel(BarrierMsg.Open.INITIAL)
        }
    }

    fun reader(key: Key, lock: CompletableDeferred<Unit>): Flow<DataWithOrigin<Output>> {
        val barrier = getBarrier(key)
        var version: Long = INITIAL_VERSION
        return barrier.asFlow()
            .onStart {
                version = versionCounter.incrementAndGet()
                delegate.acquire(key)
                lock.await()
            }
            .onCompletion {
                delegate.release(key)
            }.flatMapLatest {
                val messageArrivedAfterMe = version < it.version
                when (it) {
                    is BarrierMsg.Open -> delegate.reader(key).mapIndexed { index, output ->
                        if (index == 0 && messageArrivedAfterMe) {
                            DataWithOrigin<Output>(origin = ResponseOrigin.Fetcher, value = output)
                        } else {
                            DataWithOrigin<Output>(origin = delegate.defaultOrigin, value = output)
                        }
                    }
                    is BarrierMsg.Blocked -> {
                        it.ack.complete(Unit)
                        flowOf()
                    }
                }
            }
    }

    suspend fun write(key: Key, value: Input) {
        // TODO multiple downstream is still broken!
        //  this acking will work only for one
        //  also, it will get stuck if downstream closes but then we would close as well
        val ack = CompletableDeferred<Unit>()
        getBarrier(key).send(BarrierMsg.Blocked(versionCounter.incrementAndGet(), ack))
        delegate.write(key, value)
        getBarrier(key).send(BarrierMsg.Open(versionCounter.incrementAndGet()))
    }

    suspend fun delete(key: Key) {
        delegate.delete(key)
    }

    private sealed class BarrierMsg(
        val version: Long
    ) {
        class Blocked(version: Long, val ack: CompletableDeferred<Unit>) : BarrierMsg(version)
        class Open(version: Long) : BarrierMsg(version) {
            companion object {
                val INITIAL = Open(INITIAL_VERSION)
            }
        }
    }

    companion object {
        private const val INITIAL_VERSION = -1L
    }
}

private inline fun <T, R> Flow<T>.mapIndexed(crossinline block: (Int, T) -> R) = flow {
    this@mapIndexed.collectIndexed { index, value ->
        emit(block(index, value))
    }
}

internal data class DataWithOrigin<T>(
    val origin: ResponseOrigin,
    val value: T?
)