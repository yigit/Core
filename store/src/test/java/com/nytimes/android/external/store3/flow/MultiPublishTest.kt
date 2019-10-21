package com.nytimes.android.external.store3.flow

import com.nytimes.android.external.store3.flow2.ActorPublish
import com.nytimes.android.external.store3.flow2.log
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class MultiPublishTest {
    private val testScope = TestCoroutineScope()

    fun <T> createFlow(f : () -> Flow<T>) : Publish<T> {
        //return EndlessPublishFlow2(testScope, f)
        return ActorPublish(testScope, f)
    }

    @Test
    fun serialial_notShared() = testScope.runBlockingTest{
        var createCnt = 0
        val activeFlow = createFlow {
            createCnt ++
            when(createCnt) {
                1 -> flowOf("a", "b", "c")
                2 -> flowOf("d", "e", "f")
                else -> throw AssertionError("should not create more")
            }
        }
        assertThat(activeFlow.create().toList())
            .isEqualTo(listOf("a", "b", "c"))
        assertThat(activeFlow.create().toList())
            .isEqualTo(listOf("d", "e", "f"))
    }

    @Test
    fun slowFastCollector() = testScope.runBlockingTest{
        val activeFlow = createFlow {
            flowOf("a", "b", "c").onStart {
                // make sure both registers on time so that no one drops a value
                delay(100)
            }
        }
        val c1 = async {
            activeFlow.create().onEach {
                println("received[1] $it")
                delay(100)
            }.toList()
        }
        val c2 = async {
            activeFlow.create().onEach {
                println("received[2] $it")
                delay(200)
            }.toList()
        }
        assertThat(c1.await())
            .isEqualTo(listOf("a", "b", "c"))
        assertThat(c2.await())
            .isEqualTo(listOf("a", "b", "c"))
    }

    @Test
    fun slowDispatcher() = testScope.runBlockingTest {
        val activeFlow = createFlow {
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

    @Test
    fun lateToTheParty_arrivesAfterUpstreamClosed() = testScope.runBlockingTest {
        val activeFlow = createFlow {
            flowOf("a", "b", "c").onStart {
                delay(100)
            }
        }
        val c1 = async {
            activeFlow.create().toList()
        }
        val c2 = async {
            activeFlow.create().also {
                delay(110)
            }.toList()
        }
        assertThat(c1.await()).isEqualTo(listOf("a", "b", "c"))
        assertThat(c2.await()).isEqualTo(listOf("a", "b", "c"))
    }

    @Test
    fun lateToTheParty_arrivesBeforeUpstreamClosed() = testScope.runBlockingTest {
        var generationCounter = 0
        val activeFlow = createFlow {
            flow {
                val gen = generationCounter++
                check(gen < 2) {
                    "created one too many"
                }
                emit("a_$gen")
                delay(5)
                emit("b_$gen")
                delay(100)
            }
        }
        val c1 = async {
            activeFlow.create().onEach {
                log("[c1] $it")
            }.toList()
        }
        val c2 = async {
            activeFlow.create().also {
                delay(3)
            }.onEach {
                log("[c2] $it")
            }.toList()
        }
        val c3 = async {
            activeFlow.create().also {
                delay(20)
            }.onEach {
                log("[c3] $it")
            }.toList()
        }
        val lists = listOf(c1, c2, c3).map {
            it.await()
        }
        println("lists: $lists")
        assertThat(lists[0]).isEqualTo(listOf("a_0", "b_0"))
        assertThat(lists[1]).isEqualTo(listOf("b_0"))
        assertThat(lists[2]).isEqualTo(listOf("a_1", "b_1"))
    }
}