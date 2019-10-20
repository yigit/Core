package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel

sealed class Message<T> {
    class AddChannel<T>(val channel: Channel<DispatchValue<T>>) : Message<T>()
    class RemoveChannel<T>(val channel: Channel<DispatchValue<T>>) : Message<T>()
    class Cleanup<T> : Message<T>()
    data class DispatchValue<T>(val value: T, val delivered: CompletableDeferred<Unit>) :
        Message<T>()
}

// TODO
//  when cleaned up, check for channels that never received a value and instead of closing them
//  report them back to be re-used in another flow
/**
 * This actor helps tracking active channels and is able to dispatch values to each of them
 * in parallel. As soon as one of them receives the value, the ack in the dispatch message is
 * completed so that the sender can continue for the next item.
 */
class ChannelManager<T>(
    private val scope: CoroutineScope
) : StoreActor<Message<T>>(scope) {
    private val _hasChannelAck = CompletableDeferred<Unit>()
    private val _lostAllChannelsAck = CompletableDeferred<Unit>()

    val finished
        get() : Deferred<Unit> = _lostAllChannelsAck
    val active
        get() : Deferred<Unit> = _hasChannelAck

    private val channels = mutableListOf<Channel<Message.DispatchValue<T>>>()
    override suspend fun handle(msg: Message<T>) {
        log("received message $msg")
        when (msg) {
            is Message.AddChannel -> doAdd(msg.channel)
            is Message.RemoveChannel -> doRemove(msg.channel)
            is Message.DispatchValue -> doDispatch(msg)
            is Message.Cleanup -> doCleanup()
        }
    }

    private suspend fun doCleanup() {
        // TODO should send reason if src flow failed
        channels.forEach {
            it.close()
        }
        close()
    }

    private suspend fun doDispatch(msg: Message.DispatchValue<T>) {
        channels.forEach {
            it.send(msg)
        }
    }

    private fun doRemove(channel: Channel<Message.DispatchValue<T>>) {
        if (channels.remove(channel)) {
            if (channels.isEmpty()) {
                _lostAllChannelsAck.complete(Unit)
            }
        }
    }

    private fun doAdd(channel: Channel<Message.DispatchValue<T>>) {
        channels.add(channel)
        if (channels.size == 1) {
            _hasChannelAck.complete(Unit)
        }
    }
}