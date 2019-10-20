package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import java.nio.channels.ClosedChannelException


class SharedFlow<T>(
    private val scope: CoroutineScope,
    private val src: Flow<T>
) {
    private lateinit var collectionJob: Job
    private val channelManager = ChannelManager<T>(scope)

    val isActive
        get() = !this::collectionJob.isInitialized || collectionJob.isActive

    init {
        scope.launch {
            channelManager.active.await()
            collectionJob = scope.launch {
                src.collect {
                    val ack = CompletableDeferred<Unit>()
                    channelManager.send(Message.DispatchValue(it, ack))
                    // suspend until at least 1 receives the new value
                    ack.await()
                }
            }
            scope.launch {
                channelManager.finished.await()
                collectionJob.cancel()
            }
            collectionJob.join()
            channelManager.send(Message.Cleanup())
        }
    }

    fun create(): Flow<T> {
        return flow {
            val channel = Channel<Message.DispatchValue<T>>(Channel.UNLIMITED)
            try {
                channelManager.send(Message.AddChannel(channel))
                channel.consumeEach {
                    log("sending $it down")
                    emit(it.value)
                    it.delivered.complete(Unit)
                }
                log("DONE")
            } finally {
                try {
                    channelManager.send(Message.RemoveChannel(channel))
                } catch (closed : ClosedSendChannelException) {
                    // ignore,we are closed by it
                }

            }

        }
    }
}