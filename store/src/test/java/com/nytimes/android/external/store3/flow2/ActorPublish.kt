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
import java.util.concurrent.locks.ReentrantLock

// TODO
//  if a later created one finishes w/o any value, we should re-start the flow for that one.
//  this will allow us to nicely handle late arrivals.
class ActorPublish<T>(
    private val scope: CoroutineScope,
    private val source: () -> Flow<T>
) : Publish<T> {
    private var current: SharedFlowProducer<T>? = null
    private val LOCK = ReentrantLock()
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
            // TODO maybe let them decide on the buffer size so we dont collect too many?
            //  we could consider changing this into a randeveuz channel to avoid that and
            //  launch while dispatching? thats probably more memory expensive then keeping a
            //  list here
            val channel = Channel<Message.FlowActivity<T>>(Channel.UNLIMITED)
            var subscribed: ChannelManager<T>? = null
            try {
                while (subscribed == null) {
                    // by the time this flow starts, Publish might be already closed.
                    // so if thats the case, wait for the followup manager, there will be one!
                    try {
                        // TODO
                        //  it is probably better to pass the collector callback to the channel
                        //  where it can decide to start when it receives subscriber
                        //  it can also do the bundling of all late arrivals such that it wont
                        //  start the producer until it adds all late arrival channels into the list
                        val manager = activeChannelManager.getLatest()
                        manager.send(Message.AddChannel(channel))
                        subscribed = manager
                    } catch (closed: ClosedSendChannelException) {
                        // current is closed, get the following
                    }
                }

                channel.consumeEach {
                    log("sending $it down")
                    try {
                        when(it) {
                            is Message.DispatchValue -> emit(it.value)
                            is Message.DispatchError -> throw it.error
                        }
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
        val manager = LOCK.withLock {
            if (latestManager == null || latestManager!!.finished.isCompleted) {
                ChannelManager<T>(
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
        return manager
    }
}