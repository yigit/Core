package com.nytimes.android.external.store4

import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onStart
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@FlowPreview
@ExperimentalCoroutinesApi
internal class SourceOfTruthWithBarrier<Key, Input, Output>(
    private val delegate: SourceOfTruth<Key, Input, Output>
) {
    /**
     * Each key has a barrier so that we can block reads while writing.
     */
    private val barriers = mutableMapOf<Key, Barrier>()
    /**
     * One lock for all barrier related operations.
     */
    private val barrierLock = ReentrantLock()
    /**
     * Each message gets dispatched with a version. This ensures we won't accidentally turn on the
     * reader flow for a new reader that happens to have arrived while a write is in progress since
     * that write should be considered as a disk read for that flow, not fetcher.
     */
    private val versionCounter = AtomicLong(0)

    private fun acquireBarrier(key: Key) = barrierLock.withLock {
        barriers.getOrPut(key) {
            Barrier()
        }.also {
            it.acquire()
        }
    }

    private fun releaseBarrier(key: Key, barrier: Barrier) = barrierLock.withLock {
        val existing = barriers[key]
        check(existing === barrier) {
            "inconsistent state, a barrier has leaked"
        }
        barrier.release()
        if (!barrier.isUsed()) {
            barriers.remove(key)
        }
    }

    fun reader(key: Key, lock: CompletableDeferred<Unit>): Flow<DataWithOrigin<Output>> {
        return flow {
            val barrier = acquireBarrier(key)
            var version: Long = INITIAL_VERSION
            try {
                emitAll(barrier.asFlow()
                    .onStart {
                        version = versionCounter.incrementAndGet()
                        lock.await()
                    }.flatMapLatest {
                        val messageArrivedAfterMe = version < it.version
                        when (it) {
                            is BarrierMsg.Open -> delegate.reader(key).mapIndexed { index, output ->
                                if (index == 0 && messageArrivedAfterMe) {
                                    DataWithOrigin<Output>(
                                        origin = ResponseOrigin.Fetcher,
                                        value = output
                                    )
                                } else {
                                    DataWithOrigin<Output>(
                                        origin = delegate.defaultOrigin,
                                        value = output
                                    )
                                }
                            }
                            is BarrierMsg.Blocked -> {
                                flowOf()
                            }
                        }
                    })
            } finally {
                // we are using a finally here instead of onCompletion as there might be a
                // possibility where flow gets cancelled right before `emitAll`.
                releaseBarrier(key, barrier)
            }

        }
    }

    suspend fun write(key: Key, value: Input) {
        val barrier = acquireBarrier(key)
        try {
            barrier.send(BarrierMsg.Blocked(versionCounter.incrementAndGet()))
            delegate.write(key, value)
            barrier.send(BarrierMsg.Open(versionCounter.incrementAndGet()))
        } finally {
            releaseBarrier(key, barrier)
        }

    }

    suspend fun delete(key: Key) {
        delegate.delete(key)
    }

    private sealed class BarrierMsg(
        val version: Long
    ) {
        class Blocked(version: Long) : BarrierMsg(version)
        class Open(version: Long) : BarrierMsg(version) {
            companion object {
                val INITIAL = Open(INITIAL_VERSION)
            }
        }
    }

    // visible for testing
    internal fun barrierCount() = barrierLock.withLock {
        barriers.size
    }

    private class Barrier(
        private var usageCount: Int = 0,
        private val channel: BroadcastChannel<BarrierMsg> =
            ConflatedBroadcastChannel(BarrierMsg.Open.INITIAL)
    ) {
        suspend fun send(msg: BarrierMsg) {
            channel.send(msg)
        }

        fun asFlow() = channel.asFlow()
        // must call with barrier lock
        fun acquire() {
            usageCount++
        }

        // must call with barrier lock
        fun release() {
            usageCount--
        }

        fun isUsed() = usageCount > 0
    }

    companion object {
        private const val INITIAL_VERSION = -1L
    }
}

@ExperimentalCoroutinesApi
private inline fun <T, R> Flow<T>.mapIndexed(crossinline block: (Int, T) -> R) = flow {
    this@mapIndexed.collectIndexed { index, value ->
        emit(block(index, value))
    }
}

internal data class DataWithOrigin<T>(
    val origin: ResponseOrigin,
    val value: T?
)