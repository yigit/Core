package com.nytimes.android.external.store3.flow

import com.nytimes.android.external.store3.flow2.ActorPublish
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
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

    fun <T> createFlow(f : () -> Flow<T>) : Publish<T> {
        //return EndlessPublishFlow2(testScope, f)
        return ActorPublish(testScope, f)
    }

//    @Test
//    fun githubPublish() = runBlocking<Unit> {
//        val flow = flowOf("a", "b", "c")
//        val shared = flow.share(GlobalScope)
//        val list1 = shared.toList()
//        val list2 = shared.toList()
//        println(list1)
//        println(list2)
//        assertThat(list1).isEqualTo(listOf("a", "b", "c"))
//    }

    @Test
    fun justOne() = testScope.runBlockingTest{
        val activeFlow = createFlow {
            flowOf("a", "b", "c")
        }
        assertThat(activeFlow.create().toList())
            .isEqualTo(listOf("a", "b", "c"))
//        assertThat(activeFlow.create().toList())
//            .isEqualTo(listOf("a", "b", "c"))
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
}