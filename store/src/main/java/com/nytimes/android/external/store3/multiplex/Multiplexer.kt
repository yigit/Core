package com.nytimes.android.external.store3.multiplex

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.transform

class Multiplexer<T>(
    /**
     * The [CoroutineScope] to use for upstream subscription
     */
    private val scope: CoroutineScope,
    /**
     * The buffer size that is used only if the upstream has not complete yet.
     * Defaults to 0.
     */
    bufferSize: Int = 0,
    /**
     * Source function to create a new flow when necessary.
     */
    // TODO does this have to be a method or just a flow ? Will decide when actual implementation
    //  happens
    private val source: () -> Flow<T>
) {

    private val channelManager by lazy(LazyThreadSafetyMode.SYNCHRONIZED) {
        ChannelManager<T>(
            scope = scope,
            bufferSize = bufferSize,
            onActive = {
                SharedFlowProducer(
                    scope = scope,
                    src = source(),
                    channelManager = it
                )
            }
        )
    }

    fun create() : Flow<T> {
        val channel = Channel<ChannelManager.Message.DispatchValue<T>>(Channel.UNLIMITED)
        return channel.consumeAsFlow()
            .onStart {
                channelManager.send(
                    ChannelManager.Message.AddChannel(
                        channel
                    ) {
                        // subscription changed, record it
                        // TODO remove?
                    })
            }
            .transform {
                println("downstream received ${it.value}")
                emit(it.value)
                println("downstream emitted ${it.value}")
                it.delivered.complete(Unit)
            }.onCompletion {
                println("complted unsubscribe")
                channelManager.send(
                    ChannelManager.Message.RemoveChannel(
                        channel
                    )
                )
            }
    }

    fun create_old(): Flow<T> = flow {
        // using an unlimited channel because we'll only receive values if another collector
        // is faster than us at collection. In that case, we just want to buffer. Downstream can
        // decide if it wants to conflate
        val channel = Channel<ChannelManager.Message.DispatchValue<T>>(Channel.UNLIMITED)
        channelManager.send(
            ChannelManager.Message.AddChannel(
                channel
            ) {
                // subscription changed, record it
                // TODO remove?
            })
        val myChannelFlow = channel.consumeAsFlow()
            .transform {
                println("downstream received $it")
                emit(it.value)
                it.delivered.complete(Unit)
            }.onCompletion {
                println("complted unsubscribe")
                channelManager.send(
                    ChannelManager.Message.RemoveChannel(
                        channel
                    )
                )
            }
        emitAll(myChannelFlow)
//        try {
//            channelManager.send(
//                ChannelManager.Message.AddChannel(
//                    channel
//                ) {
//                    // subscription changed, record it
//                    // TODO remove?
//                })
//            channel.consumeEach {
//                try {
//                    println("downstream received ${it.value}")
//                    // send the value to downstream
//                    emit(it.value)
//                    println("downstream emitted ${it.value}")
//                } finally {
//                    println("downstream acking msg")
//                    // once it is done, mark it as delivered so that upstream asks for a new
//                    // value
//                    it.delivered.complete(Unit)
//                }
//            }
//            println("downstream done due to upstream being done")
//        } finally {
//            try {
//                // if we ever subscribed, send an unsubscribe request to the channel we were
//                // subscribed to.
//                // TODO we might be a leftover such that we might send the remove to the
//                //  previous channel by mistake.
//                channelManager.send(
//                    ChannelManager.Message.RemoveChannel(
//                        channel
//                    )
//                )
//            } catch (closed: ClosedSendChannelException) {
//                // ignore as we might be closed by that channel already
//            }
//        }
    }
}