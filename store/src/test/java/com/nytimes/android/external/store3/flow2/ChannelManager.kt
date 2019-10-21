package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel

sealed class Message<T> {
    /**
     * Add a new channel, that means a new downstream subscriber
     */
    class AddChannel<T>(val channel: Channel<FlowActivity<T>>) : Message<T>()

    /**
     * Add multiple channels. Happens when we are carrying over leftovers from a previous
     * manager
     */
    class AddLeftovers<T>(val leftovers: List<Channel<FlowActivity<T>>>) : Message<T>()

    /**
     * Remove a downstream subscriber, that means it completed
     */
    class RemoveChannel<T>(val channel: Channel<FlowActivity<T>>) : Message<T>()

    /**
     * Cleanup all channels, the producer is done
     */
    class Cleanup<T> : Message<T>()

    /**
     * Base class for all flow activities. It is either an error or a value
     */
    open class FlowActivity<T>(val delivered: CompletableDeferred<Unit>) : Message<T>()

    /**
     * Upstream dispatched a new value, send it to all downstream items
     */
    class DispatchValue<T>(
        /**
         * The value dispatched by the upstream
         */
        val value: T,
        /**
         * Ack that is completed by all receiver. Upstream producer will await this before asking
         * for a new value from upstream
         */
        delivered: CompletableDeferred<Unit>
    ) : FlowActivity<T>(delivered)

    /**
     * Upstream dispatched an error. We should send it to all downstream items.
     */
    class DispatchError<T>(
        val error: Throwable,
        delivered: CompletableDeferred<Unit>
    ) : FlowActivity<T>(delivered)
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
    scope: CoroutineScope,
    private val onActive: (ChannelManager<T>) -> Unit,
    private val onClosed: suspend (/*has leftovers*/ChannelManager<T>, Boolean) -> Unit
) : StoreActor<Message<T>>(scope) {
    private var next = CompletableDeferred<ChannelManager<T>>()
    private val _hasChannelAck = CompletableDeferred<Unit>()
    private val _lostAllChannelsAck = CompletableDeferred<Unit>()
    private lateinit var leftovers: MutableList<Channel<Message.FlowActivity<T>>>
    // set when we first dispatch a value to be able to track leftovers
    private var dispatchedValue = false
    val finished
        get() : Deferred<Unit> = _lostAllChannelsAck
    val active
        get() : Deferred<Unit> = _hasChannelAck

    suspend fun next() = next.await()

    private val channels = mutableListOf<ChannelEntry<T>>()
    override suspend fun handle(msg: Message<T>) {
        log("received message $msg")
        when (msg) {
            is Message.AddLeftovers -> doAddLefovers(msg.leftovers)
            is Message.AddChannel -> doAdd(msg.channel)
            is Message.RemoveChannel -> doRemove(msg.channel)
            is Message.FlowActivity -> doDispatch(msg)
            is Message.Cleanup -> doCleanup()
        }
    }

    private suspend fun doCleanup() {
        // TODO should send reason if src flow failed
        val leftovers = mutableListOf<Channel<Message.FlowActivity<T>>>()
        channels.forEach {
            if (it.receivedValue) {
                it.channel.close()
            } else if (dispatchedValue) {
                // we dispatched a value but this channel didn't receive so put it into leftovers
                leftovers.add(it.channel)
            } else {
                // upstream didn't dispatch
                it.channel.close()
            }
        }
        this.leftovers = leftovers // keep leftovers, they'll be cleaned in setNext of the next one
        channels.clear() // empty references
        close()
        _lostAllChannelsAck.complete(Unit)
        onClosed(this, leftovers.isNotEmpty())
    }

    private suspend fun doDispatch(msg: Message.FlowActivity<T>) {
        dispatchedValue = true
        channels.forEach {
            it.receivedValue = true
            it.channel.send(msg)
        }
    }

    private fun doRemove(channel: Channel<Message.FlowActivity<T>>) {
        val index = channels.indexOfFirst {
            it.channel === channel
        }
        if (index >= 0) {
            channels.removeAt(index)
            if (channels.isEmpty()) {
                _lostAllChannelsAck.complete(Unit)
            }
        }
    }

    private fun doAddLefovers(leftovers: List<Channel<Message.FlowActivity<T>>>) {
        val allNew = leftovers.all { channel ->
            channels.none {
                it.channel === channel
            }
        }

        check(allNew) {
            "some channels are already in the list (complete list): $leftovers."
        }
        leftovers.forEach { channel ->
            channels.add(ChannelEntry(channel))
        }

        if (_hasChannelAck.isActive && channels.size > 0) {
            _hasChannelAck.complete(Unit)
            onActive(this)
        }
    }

    private fun doAdd(channel: Channel<Message.FlowActivity<T>>) {
        val new = channels.none {
            it.channel === channel
        }
        check(new) {
            "$channel is already in the list."
        }
        channels.add(ChannelEntry(channel))
        if (_hasChannelAck.isActive && channels.size == 1) {
            _hasChannelAck.complete(Unit)
            onActive(this)
        }
    }

    fun setNext(channelManager: ChannelManager<T>): Unit {
        check(!next.isCompleted) {
            "next is already set!!!"
        }
        next.complete(channelManager)
        // we don't check for closed here because it shouldn't be closed by now
        if (leftovers.isNotEmpty()) {
            val accepted = channelManager.offer(Message.AddLeftovers(leftovers))
            check(accepted) {
                "couldn't carry over leftovers"
            }
        }
        leftovers.clear()
    }

    private class ChannelEntry<T>(
        val channel: Channel<Message.FlowActivity<T>>,
        var receivedValue: Boolean = false
    )
}