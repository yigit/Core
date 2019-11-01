package com.nytimes.android.external.store3.multiplex

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch

/**
 * Simple actor implementation because coroutines actor is being deprecated ¯\_(ツ)_/¯
 */
@ExperimentalCoroutinesApi
abstract class StoreRealActor<T>(
    scope: CoroutineScope
) {
    val inboundChannel : SendChannel<T>
    init {
        inboundChannel = scope.actor<T>(
            capacity = 0
        ) {
            for(msg in channel) {
                handle(msg)
            }
        }
    }

    abstract suspend fun handle(msg: T)

    suspend fun send(msg: T) {
        Dispatchers.Main.immediate
        println("sending msg $msg to $this")
        inboundChannel.send(msg)
    }

    fun offer(msg: T) : Boolean {
        return inboundChannel.offer(msg)
    }
//
//    fun close() {
//        inboundChannel.close()
//    }
}
