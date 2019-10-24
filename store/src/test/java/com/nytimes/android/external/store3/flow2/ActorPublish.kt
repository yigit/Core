package com.nytimes.android.external.store3.flow2

import com.nytimes.android.external.store3.flow.Publish
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class ActorPublish<T>(
    private val scope: CoroutineScope,
    private val source: () -> Flow<T>
) : Publish<T> {
    private val activeChannelManager = ActiveChannelTracker<T>(
        scope = scope,
        onCreateProducer = { channelManager ->
            log("creating producer")
            SharedFlowProducer(
                scope = scope,
                src = source(),
                channelManager = channelManager
            ).start()
        }
    )

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
                    log("sending $it down")
                    try {
                        emit(it.value)
                    } finally {
                        it.delivered.complete(Unit)
                    }
                }
                log("DONE")
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

internal class ActiveChannelTracker<T>(
    private val scope: CoroutineScope,
    private val onCreateProducer: (ChannelManager<T>) -> Unit
) {
    // TODO
    //  this should be better at ensuring there is only 1 channel manager, blocking if necessary
    //  and then always ensure there is a latest one that can be passed down to the individual
    //  collectors. Since it is lazy, should be possible to do in suspend blocks
    private val LOCK = Mutex()
    private var latestManager: ChannelManager<T>? = null
    suspend fun getLatest(): ChannelManager<T> {
        return LOCK.withLock {
            if (latestManager == null || latestManager!!.finished.isCompleted) {
                ChannelManager(
                    scope = scope,
                    onActive = onCreateProducer,
                    onClosed = { _, needsRestart ->
                        if (needsRestart) {
                            // TODO re-entry?
                            getLatest()
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