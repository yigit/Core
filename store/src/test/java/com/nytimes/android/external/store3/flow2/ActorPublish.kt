package com.nytimes.android.external.store3.flow2

import com.nytimes.android.external.store3.flow.Publish
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A publish implementation very specific to the Store use case.
 *
 * It shares an upstream between multiple downstream subscribers.
 *
 * If a downstream subscriber arrives late such that it does not receive any values, even though
 * there were values published before, a new upstream is created to ensure that no downstream is
 * left without a value for arriving late.
 *
 * We prefer this model instead of caching last value to ensure that any new subscriber gets values
 * that were emitted after it subscribed.
 *
 * For certain use cases, buffering might be preferred (like a database). For those, [bufferSize]
 * can be set to a value greater than 0. Note that this buffer is used only if upstream is still
 * open, if upstream closed, there is no buffering. (e.g. we don't want to go into caching business
 * here)
 */

@ExperimentalCoroutinesApi
class ActorPublish<T>(
    private val scope: CoroutineScope,
    bufferSize: Int = 0,
    private val source: () -> Flow<T>
) : Publish<T> {
    private val activeChannelManager = ActiveChannelTracker<T>(
        scope = scope,
        bufferSize = bufferSize,
        onCreateProducer = { channelManager ->
            SharedFlowProducer(
                scope = scope,
                src = source(),
                channelManager = channelManager
            ).start()
        }
    )

    init {
        check(bufferSize >= 0) {
            "buffer should be 0 or positive"
        }
    }

    override fun create(): Flow<T> {
        return flow {
            // using an unlimited channel because we'll only receive values if another collector
            // is fast at collection. In that case, we just want to buffer. Downstream can decide
            // if it wants to conflate
            val channel = Channel<Message.DispatchValue<T>>(Channel.UNLIMITED)
            var subscribed: ChannelManager<T>? = null
            try {
                while (subscribed == null) {
                    // by the time this flow starts, Publish might be already closed.
                    // so if thats the case, wait for the followup manager, there will be one!
                    try {
                        val manager = activeChannelManager.getLatest()
                        manager.send(Message.AddChannel(channel) {
                            subscribed = it
                        })
                        subscribed = manager
                    } catch (closed: ClosedSendChannelException) {
                        // current is closed, get the following
                    }
                }

                channel.consumeEach {
                    try {
                        emit(it.value)
                    } finally {
                        it.delivered.complete(Unit)
                    }
                }
            } finally {
                try {
                    subscribed?.send(Message.RemoveChannel(channel))
                } catch (closed: ClosedSendChannelException) {
                    // ignore,we are closed by it
                }
            }
        }
    }
}

/**
 * This helper ensures there is always 1 and only 1 ChannelManager that is currently active.
 * It might be down to zero if active channel manager finishes w/o any leftovers.
 */
internal class ActiveChannelTracker<T>(
    private val scope: CoroutineScope,
    private val bufferSize: Int,
    private val onCreateProducer: (ChannelManager<T>) -> Unit
) {
    private val lock = Mutex()
    private var latestManager: ChannelManager<T>? = null
    suspend fun getLatest(): ChannelManager<T> {
        return lock.withLock {
            if (latestManager == null || latestManager!!.finished.isCompleted) {
                ChannelManager(
                    scope = scope,
                    onActive = onCreateProducer,
                    bufferSize = bufferSize,
                    onClosed = { _, needsRestart ->
                        if (needsRestart) {
                            // TODO re-entry?
                            getLatest()
                        } else {
                            lock.withLock {
                                latestManager = null
                            }
                        }
                    }
                ).also {
                    latestManager?.setNext(it)
                    latestManager = it
                }
            } else {
                latestManager!!
            }
        }
    }
}