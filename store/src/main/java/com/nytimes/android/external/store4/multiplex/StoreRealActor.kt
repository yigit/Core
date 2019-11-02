package com.nytimes.android.external.store4.multiplex

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor

/**
 * Simple actor implementation because coroutines actor is being deprecated ¯\_(ツ)_/¯
 */
@ExperimentalCoroutinesApi
abstract class StoreRealActor<T>(
    scope: CoroutineScope
) {
    val inboundChannel: SendChannel<T>

    init {
        inboundChannel = scope.actor(
            capacity = 0
        ) {
            for (msg in channel) {
                handle(msg)
            }
        }
    }

    abstract suspend fun handle(msg: T)

    suspend fun send(msg: T) {
        Dispatchers.Main.immediate
        inboundChannel.send(msg)
    }
}
