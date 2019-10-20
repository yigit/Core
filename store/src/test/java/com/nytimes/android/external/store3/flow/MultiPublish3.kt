package com.nytimes.android.external.store3.flow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch

abstract class StoreActor<T>(
    private val scope: CoroutineScope
) {
    private val inboundChannel = Channel<T>(Channel.UNLIMITED)

    init {
        scope.launch {
            inboundChannel.consumeEach {
                handle(it)
            }
        }
    }

    abstract suspend fun handle(msg: T)

    suspend fun send(msg: T) {
        inboundChannel.send(msg)
    }

    suspend fun close() {
        inboundChannel.close()
    }
}

/**
 * Controller that can track active channels
 * Send items to each of them w/o blocking
 */
class ChannelManager<T> {

}