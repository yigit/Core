package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ChannelManagerTest {
    private val scope = TestCoroutineScope()
    private val manager = ChannelManager<String>(scope)

    @Test
    fun simple() = scope.runBlockingTest {
        val collection = async {
            val channel = Channel<Message.DispatchValue<String>>(Channel.RENDEZVOUS)
            try {
                manager.send(Message.AddChannel(channel))
                channel.consumeAsFlow().take(2).toList()
            } finally {
                manager.send(Message.RemoveChannel(channel))
            }
        }
        manager.active.await()
        val ack1 = CompletableDeferred<Unit>()
        manager.send(Message.DispatchValue("a", ack1))

        val ack2 = CompletableDeferred<Unit>()
        manager.send(Message.DispatchValue("b", ack2))
        manager.finished.await()
        assertThat(collection.await()).isEqualTo(listOf("a", "b"))
    }
}