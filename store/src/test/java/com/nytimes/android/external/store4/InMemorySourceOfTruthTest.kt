package com.nytimes.android.external.store4

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@ExperimentalCoroutinesApi
@RunWith(JUnit4::class)
class InMemorySourceOfTruthTest {
    private val testScope = TestCoroutineScope()
    private val source: SourceOfTruth<Int, String, String> = InMemorySourceOfTruth()

    @Test
    fun simple() = testScope.runBlockingTest {
        val receivedFirst = CompletableDeferred<Unit>()
        val collector = async {
            source
                .reader(1)
                .onEach {
                    if (it != null) {
                        receivedFirst.complete(Unit)
                    }
                }
                .take(4)
                .toList()
        }
        source.write(1, "a")
        receivedFirst.await()
        assertThat(source.getSize()).isEqualTo(1)
        source.write(2, "b")
        assertThat(source.getSize()).isEqualTo(1)
        source.write(1, "c")
        source.write(1, "d")
        assertThat(collector.await()).isEqualTo(listOf(null, "a", "c", "d"))
        // flow is done so there should be nothing inside
        assertThat(source.getSize()).isEqualTo(0)
    }

    @Test
    fun multiple() = testScope.runBlockingTest {
        val collector1 = async {
            source
                .reader(1)
                .take(4)
                .toList()
        }

        val collector2 = async {
            source
                .reader(1)
                .take(4)
                .toList()
        }
        source.write(1, "a")
        assertThat(source.getSize()).isEqualTo(1)
        source.write(2, "b")
        assertThat(source.getSize()).isEqualTo(1)
        source.write(1, "c")
        source.write(1, "d")
        assertThat(collector1.await()).isEqualTo(listOf(null, "a", "c", "d"))
        assertThat(collector2.await()).isEqualTo(listOf(null, "a", "c", "d"))
        // flow is done so there should be nothing inside
        assertThat(source.getSize()).isEqualTo(0)
    }

    @Test
    fun multiple_lateArrival() = testScope.runBlockingTest {
        val receivedC = CompletableDeferred<Unit>()
        val canDispatchD = CompletableDeferred<Unit>()
        val collector1 = async {
            source
                .reader(1)
                .onEach {
                    if (it == "c") {
                        receivedC.complete(Unit)
                    }
                }
                .take(4)
                .toList()
        }
        val collector2 = async {
            receivedC.await()
            source
                .reader(1)
                .onEach {
                    if (it == "c") {
                        canDispatchD.complete(Unit)
                    }
                }
                .take(2)
                .toList()
        }
        source.write(1, "a")
        source.write(2, "b")
        source.write(1, "c")
        canDispatchD.await()
        source.write(1, "d")
        assertThat(collector1.await()).isEqualTo(listOf(null, "a", "c", "d"))
        assertThat(collector2.await()).isEqualTo(listOf("c", "d"))
        // flow is done so there should be nothing inside
        assertThat(source.getSize()).isEqualTo(0)
    }

    @Test
    fun differentKeys() = testScope.runBlockingTest {
        val collector1 = async {
            source
                .reader(1)
                .take(4)
                .toList()
        }
        val collector2 = async {
            source
                .reader(2)
                .take(3)
                .toList()
        }
        source.write(1, "a1")
        source.write(2, "a2")
        source.write(2, "b2")
        source.write(1, "b1")
        source.write(1, "c1")
        assertThat(collector1.await()).isEqualTo(listOf(null, "a1", "b1", "c1"))
        assertThat(collector2.await()).isEqualTo(listOf(null, "a2", "b2"))
        assertThat(source.getSize()).isEqualTo(0)
    }
}