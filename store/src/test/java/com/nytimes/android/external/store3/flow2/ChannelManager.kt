package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import java.util.ArrayDeque
import java.util.Collections

sealed class Message<T> {
    /**
     * Add a new channel, that means a new downstream subscriber
     */
    class AddChannel<T>(
        val channel: Channel<DispatchValue<T>>,
        val onSubscribed: (ChannelManager<T>) -> Unit
    ) : Message<T>()

    /**
     * Add multiple channels. Happens when we are carrying over leftovers from a previous
     * manager
     */
    internal class AddLeftovers<T>(val leftovers: List<ChannelManager.ChannelEntry<T>>) :
        Message<T>()

    /**
     * Remove a downstream subscriber, that means it completed
     */
    class RemoveChannel<T>(val channel: Channel<DispatchValue<T>>) : Message<T>()

    /**
     * Cleanup all channels, the producer is done
     */
    class Cleanup<T> : Message<T>()

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
        val delivered: CompletableDeferred<Unit>
    ) : Message<T>()

    /**
     * Upstream dispatched a new value, send it to all downstream items
     */
    class DispatchError<T>(
        /**
         * The error sent by the upstream
         */
        val error: Throwable
    ) : Message<T>()
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
    bufferSize: Int,
    private val onActive: (ChannelManager<T>) -> Unit,
    private val onClosed: suspend (/*has leftovers*/ChannelManager<T>, Boolean) -> Unit
) : StoreActor<Message<T>>(scope) {
    private val buffer = Buffer<T>(bufferSize)
    private var next = CompletableDeferred<ChannelManager<T>>()
    private val _hasChannelAck = CompletableDeferred<Unit>()
    private val _lostAllChannelsAck = CompletableDeferred<Unit>()
    private lateinit var leftovers: MutableList<ChannelEntry<T>>
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
            is Message.AddChannel -> doAdd(msg)
            is Message.RemoveChannel -> doRemove(msg.channel)
            is Message.DispatchValue -> doDispatchValue(msg)
            is Message.DispatchError -> doDispatchError(msg)
            is Message.Cleanup -> doCleanup()
        }
    }

    private suspend fun doCleanup() {
        // TODO should send reason if src flow failed
        val leftovers = mutableListOf<ChannelEntry<T>>()
        channels.forEach {
            if (it.receivedValue) {
                it.close()
            } else if (dispatchedValue) {
                // we dispatched a value but this channel didn't receive so put it into leftovers
                leftovers.add(it)
            } else {
                // upstream didn't dispatch
                it.close()
            }
        }
        this.leftovers = leftovers // keep leftovers, they'll be cleaned in setNext of the next one
        channels.clear() // empty references
        close()
        _lostAllChannelsAck.complete(Unit)
        onClosed(this, leftovers.isNotEmpty())
    }

    private suspend fun doDispatchValue(msg: Message.DispatchValue<T>) {
        buffer.add(msg)
        dispatchedValue = true
        channels.forEach {
            it.dispatchValue(msg)
        }
    }

    private fun doDispatchError(msg: Message.DispatchError<T>) {
        // dispatching error is as good as dispatching value
        dispatchedValue = true
        channels.forEach {
            it.dispatchError(msg.error)
        }
    }

    private fun doRemove(channel: Channel<Message.DispatchValue<T>>) {
        val index = channels.indexOfFirst {
            it.hasChannel(channel)
        }
        if (index >= 0) {
            channels.removeAt(index)
            if (channels.isEmpty()) {
                _lostAllChannelsAck.complete(Unit)
            }
        }
    }

    private suspend fun doAddLefovers(leftovers: List<ChannelEntry<T>>) {
        leftovers.forEachIndexed { index, channelEntry ->
            addEntry(
                entry = channelEntry.copy(_receivedValue = false),
                notifySize = index == leftovers.size - 1
            )
        }
    }

    private suspend fun doAdd(msg: Message.AddChannel<T>) {
        addEntry(
            entry = ChannelEntry(
                channel = msg.channel,
                onSubscribed = msg.onSubscribed
            ),
            notifySize = true
        )
    }

    private suspend fun addEntry(entry: ChannelEntry<T>, notifySize: Boolean) {
        val new = channels.none {
            it.hasChannel(entry)
        }
        check(new) {
            "$entry is already in the list."
        }
        check(!entry.receivedValue) {
            "$entry already received a value"
        }
        channels.add(entry)

        entry.onSubscribed(this)
        // if there is anything in the buffer, send it
        buffer.items.forEach {
            entry.dispatchValue(it)
        }
        if (notifySize) {
            if (_hasChannelAck.isActive && channels.size == 1) {
                _hasChannelAck.complete(Unit)
                onActive(this)
            }
        }
    }

    fun setNext(channelManager: ChannelManager<T>) {
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

    internal data class ChannelEntry<T>(
        private val channel: Channel<Message.DispatchValue<T>>,
        private var _receivedValue: Boolean = false,
        // called back when a downstream's Add request is handled
        val onSubscribed: (ChannelManager<T>) -> Unit
    ) {
        val receivedValue
            get() = _receivedValue

        suspend fun dispatchValue(value: Message.DispatchValue<T>) {
            _receivedValue = true
            channel.send(value)
        }

        fun dispatchError(error: Throwable) {
            _receivedValue = true
            channel.close(error)
        }

        fun close() {
            channel.close()
        }

        fun hasChannel(channel: Channel<Message.DispatchValue<T>>) = this.channel === channel
        fun hasChannel(entry: ChannelEntry<T>) = this.channel === entry.channel
    }
}

private interface Buffer<T> {
    fun add(item: Message.DispatchValue<T>)
    val items: Collection<Message.DispatchValue<T>>
}

private class NoBuffer<T> : Buffer<T> {
    override val items: Collection<Message.DispatchValue<T>>
        get() = Collections.emptyList()


    override fun add(item: Message.DispatchValue<T>) {
        // ignore
    }
}

private fun <T> Buffer(limit: Int): Buffer<T> = if (limit > 0) {
    BufferImpl(limit)
} else {
    NoBuffer<T>()
}

private class BufferImpl<T>(private val limit: Int) : Buffer<T> {
    override val items = ArrayDeque<Message.DispatchValue<T>>(limit.coerceAtMost(10))
    override fun add(item: Message.DispatchValue<T>) {
        while (items.size >= limit) {
            items.pollFirst()
        }
        items.offerLast(item)
    }
}