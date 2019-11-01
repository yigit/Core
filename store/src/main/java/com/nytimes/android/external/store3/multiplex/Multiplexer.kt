package com.nytimes.android.external.store3.multiplex

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.transform

@FlowPreview
@ExperimentalCoroutinesApi
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

    fun create(): Flow<T> {
        val channel = Channel<ChannelManager.Message.DispatchValue<T>>(Channel.UNLIMITED)
        return channel.consumeAsFlow()
            .onStart {
                channelManager.send(
                    ChannelManager.Message.AddChannel(
                        channel
                    ))
            }
            .transform {
                emit(it.value)
                it.delivered.complete(Unit)
            }.onCompletion {
                channelManager.send(
                    ChannelManager.Message.RemoveChannel(
                        channel
                    )
                )
            }
    }
}