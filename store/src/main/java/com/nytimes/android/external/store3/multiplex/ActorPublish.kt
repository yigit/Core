package com.nytimes.android.external.store3.multiplex

import com.nytimes.android.external.store3.multiplex.ChannelManager.Message.AddChannel
import com.nytimes.android.external.store3.multiplex.ChannelManager.Message.DispatchValue
import com.nytimes.android.external.store3.multiplex.ChannelManager.Message.RemoveChannel
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
 * open, if upstream is closed, there is no buffering. (e.g. we don't want to go into caching
 * business here)
 */

@ExperimentalCoroutinesApi
class ActorPublish<T>(
    /**
     * The [CoroutineScope] to use for upstream subscription
     */
    private val scope: CoroutineScope,
    /**
     * The buffer size that is used only if the upstream has not complete yet.
     * Defaults to 0.
     */
    bufferSize: Int = 0,
    /**
     * Source function to create a new flow when necessary.
     */
    // TODO does this have to be a method or just a flow ? Will decide when actual implementation
    //  happens
    private val source: () -> Flow<T>
)  {
    /**
     * Helper class that tracks and ensures there only 1 channel manager at a time.
     */
    private val activeChannelManager =
        ActiveChannelTracker<T>(
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

    /**
     * Creates a new flow that shares the upstream.
     */
    fun create(): Flow<T> {
        return flow {
            // using an unlimited channel because we'll only receive values if another collector
            // is faster than us at collection. In that case, we just want to buffer. Downstream can
            // decide if it wants to conflate
            val channel = Channel<DispatchValue<T>>(Channel.UNLIMITED)
            // we track the [ChannelManager] that we are subscribed to. It might change if our
            // subscription is moved between channel managers if we arrived too late to the previous
            // one and there is no caching enabled.
            var subscribed: ChannelManager<T>? = null
            try {
                while (subscribed == null) {
                    // by the time this flow starts, Publish might be already closed.
                    // so if thats the case, wait for the followup manager, there will be one!
                    try {
                        val manager = activeChannelManager.getLatest()
                        manager.send(
                            AddChannel(
                                channel
                            ) {
                                // subscription changed, record it
                                subscribed = it
                            })
                    } catch (closed: ClosedSendChannelException) {
                        // current is closed, get the following
                    }
                }

                channel.consumeEach {
                    try {
                        // send the value to downstream
                        emit(it.value)
                    } finally {
                        // once it is done, mark it as delivered so that upstream asks for a new
                        // value
                        it.delivered.complete(Unit)
                    }
                }
            } finally {
                try {
                    // if we ever subscribed, send an unsubscribe request to the channel we were
                    // subscribed to.
                    subscribed?.send(
                        RemoveChannel(
                            channel
                        )
                    )
                } catch (closed: ClosedSendChannelException) {
                    // ignore as we might be closed by that channel already
                }
            }
        }
    }

    /**
     * This helper ensures there is always 1 and only 1 ChannelManager that is currently active.
     * It might be down to zero if active channel manager finishes w/o any leftovers.
     */
    private class ActiveChannelTracker<T>(
        /**
         * The scope to launch the upstream consumer
         */
        private val scope: CoroutineScope,
        /**
         * Buffer size to use in the upstream
         */
        private val bufferSize: Int,
        /**
         * Every time we create a new Channel, we call this callback to create a producer for it.
         */
        private val onCreateProducer: (ChannelManager<T>) -> Unit
    ) {
        /**
         * The lock over [latestManager]
         */
        private val lock = Mutex()
        /**
         * Currently active manager. This might hold the previous manager as well if it finished
         * with leftovers. In that case, we'll always request a new one.
         */
        private var latestManager: ChannelManager<T>? = null

        /**
         * Returns the currently active channel manager or creates a new one that also carries over
         * any leftover downstream channels.
         */
        suspend fun getLatest(): ChannelManager<T> {
            return lock.withLock {
                if (latestManager == null || latestManager!!.finished.isCompleted) {
                    ChannelManager(
                        scope = scope,
                        onActive = onCreateProducer,
                        bufferSize = bufferSize,
                        onClosed = { needsRestart ->
                            if (needsRestart) {
                                // this recursion is fine because ChannelManager would not close
                                // before we create a producer for it, which won't happen until it
                                // starts by receiving some messages
                                getLatest()
                            } else {
                                lock.withLock {
                                    latestManager = null
                                }
                            }
                        }
                    ).also {
                        // set this manager as the followup to the previous one so that it can
                        // carry over any leftovers
                        latestManager?.setNext(it)
                        latestManager = it
                    }
                } else {
                    latestManager!!
                }
            }
        }
    }
}