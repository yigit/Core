package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch

abstract class StoreActor<T>(
    private val scope: CoroutineScope
) {
    private val inboundChannel = Channel<Packet<T>>(Channel.UNLIMITED)
    init {
        scope.launch {
            inboundChannel.consumeEach {
                handle(it.data)
                it.ack.complete(Unit)
            }
            log("inbound channel finished")
        }
    }

    abstract suspend fun handle(msg: T)

    suspend fun send(msg: T) {
        val ack = CompletableDeferred<Unit>()
        val packet = Packet(msg, ack)
        inboundChannel.send(packet)
        ack.await()
    }

    fun offer(msg: T) {
        val ack = CompletableDeferred<Unit>()
        val packet = Packet(msg, ack)
        inboundChannel.offer(packet)
    }

    suspend fun close() {
        inboundChannel.close()
    }

    private class Packet<T>(
        val data: T,
        val ack: CompletableDeferred<Unit>
    )
}
