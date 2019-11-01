package com.nytimes.android.external.store3.multiplex

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch

/**
 * Simple actor implementation because coroutines actor is being deprecated ¯\_(ツ)_/¯
 */
@ExperimentalCoroutinesApi
abstract class StoreActor<T>(
    scope: CoroutineScope
) {
    private val inboundChannel = Channel<Packet<T>>(Channel.UNLIMITED)
    init {
        scope.launch {
            try {
                inboundChannel.consumeEach {
                    handle(it.data)
                    it.ack.complete(Unit)
                }
            } finally {
                inboundChannel.close()
                onClose()
            }
        }
    }

    abstract suspend fun onClose()
    abstract suspend fun handle(msg: T)

    suspend fun send(msg: T) {
        Dispatchers.Main.immediate
        println("sending msg $msg to $this")
        val ack = CompletableDeferred<Unit>()
        val packet = Packet(msg, ack)
        inboundChannel.send(packet)
        ack.await()
    }

    fun offer(msg: T) : Boolean {
        val ack = CompletableDeferred<Unit>()
        val packet = Packet(msg, ack)
        return inboundChannel.offer(packet)
    }

    fun close() {
        inboundChannel.close()
    }

    private class Packet<T>(
        val data: T,
        val ack: CompletableDeferred<Unit>
    )
}
