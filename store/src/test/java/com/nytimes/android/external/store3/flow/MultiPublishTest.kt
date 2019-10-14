package com.nytimes.android.external.store3.flow

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.CountDownLatch

@RunWith(JUnit4::class)
class MultiPublishTest {
    private val testScope = TestCoroutineScope()
    @Test
    fun justOne() = testScope.runBlockingTest{
        val activeFlow = EndlessPublishFlow<String>(testScope) {
            flowOf("a", "b", "c")
        }
        assertThat(activeFlow.create().toList())
            .isEqualTo(listOf("a", "b", "c"))
        assertThat(activeFlow.create().toList())
            .isEqualTo(listOf("a", "b", "c"))
    }

    @Test
    fun slowFastCollector() = testScope.runBlockingTest{
        val activeFlow = EndlessPublishFlow<String>(testScope) {
            flowOf("a", "b", "c")
        }
        val bothRegistered = CountDownLatch(2)
        val c1 = async {
            activeFlow.create().also {
                bothRegistered.countDown()
            }.onEach {
                println("received[1] $it")
                delay(100)
            }.toList()
        }
        val c2 = async {
            activeFlow.create().also {
                bothRegistered.countDown()
            }.onEach {
                println("received[2] $it")
                delay(200)
            }.toList()
        }
        bothRegistered.await()
        assertThat(c1.await())
            .isEqualTo(listOf("a", "b", "c"))
        assertThat(c2.await())
            .isEqualTo(listOf("a", "b", "c"))
    }

    @Test
    fun slowDispatcher() = testScope.runBlockingTest {
        val activeFlow = EndlessPublishFlow(testScope) {
            flowOf("a", "b", "c").onEach {
                delay(100)
            }
        }
        val c1 = async {
            activeFlow.create().toList()
        }
        val c2 = async {
            activeFlow.create().toList()
        }
        assertThat(c1.await()).isEqualTo(listOf("a", "b", "c"))
        assertThat(c2.await()).isEqualTo(listOf("a", "b", "c"))
    }
}