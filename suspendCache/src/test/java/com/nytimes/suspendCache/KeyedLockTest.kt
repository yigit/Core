package com.nytimes.suspendCache

import com.com.nytimes.suspendCache.KeyedLock
import kotlinx.coroutines.*
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

// TODO: not used in v1, remove before merge
@ExperimentalCoroutinesApi
@RunWith(JUnit4::class)
class KeyedLockTest {
    private val lock = KeyedLock<String>()
    private val testScope = TestCoroutineScope()

    @After
    fun checkCleanup() {
        testScope.advanceUntilIdle()
        assertThat(testScope.uncaughtExceptions).isEmpty()
    }
    @Test
    fun multipleReaders() = testScope.runBlockingTest {
        val readerCount = 10
        var activeReaders = 0
        println("running")
        val finish = CompletableDeferred<Unit>()
        val reads = (0 until readerCount).map {
            async {
                lock.withReadLock("foo") {
                    activeReaders++
                    finish.await()
                    activeReaders--
                }
            }
        }
        testScope.advanceUntilIdle()
        assertThat(activeReaders).isEqualTo(readerCount)
        finish.complete(Unit)
        testScope.advanceUntilIdle()
        assertThat(activeReaders).isEqualTo(0)
    }

    @Test
    fun writerWhileReaderActive() = testScope.runBlockingTest {
        val finishReader = CompletableDeferred<Unit>()
        val reader = async {
            lock.withReadLock("foo") {
                finishReader.await()
                true
            }
        }
        testScope.advanceUntilIdle()
        val writer = async {
            lock.withWriteLock("foo") {
                true
            }
        }
        testScope.advanceUntilIdle()
        assertThat(writer.isCompleted).isFalse()
        finishReader.complete(Unit)
        testScope.advanceUntilIdle()
        assertThat(reader.await()).isTrue()
        assertThat(writer.await()).isTrue()
    }

    @Test
    fun readerWhileWriterActive() = testScope.runBlockingTest {
        val finishWriter = CompletableDeferred<Unit>()
        val writer = async {
            lock.withWriteLock("foo") {
                finishWriter.await()
                true
            }
        }
        testScope.advanceUntilIdle()
        val reader = async {
            lock.withReadLock("foo") {
                true
            }
        }
        testScope.advanceUntilIdle()
        assertThat(reader.isCompleted).isFalse()
        finishWriter.complete(Unit)
        testScope.advanceUntilIdle()
        assertThat(writer.await()).isTrue()
        assertThat(reader.await()).isTrue()
    }

    @Test
    fun writerWhileWriterActive() = testScope.runBlockingTest {
        val finishWriter = CompletableDeferred<Unit>()
        val writer = async {
            lock.withWriteLock("foo") {
                finishWriter.await()
                true
            }
        }
        testScope.advanceUntilIdle()
        val writer2 = async {
            lock.withWriteLock("foo") {
                true
            }
        }
        testScope.advanceUntilIdle()
        assertThat(writer2.isCompleted).isFalse()
        finishWriter.complete(Unit)
        testScope.advanceUntilIdle()
        assertThat(writer.await()).isTrue()
        assertThat(writer2.await()).isTrue()
    }

    @Test
    fun writerWhileReaderActive_differentKey() = testScope.runBlockingTest {
        val finishReader = CompletableDeferred<Unit>()
        val reader = async {
            lock.withReadLock("foo") {
                finishReader.await()
                true
            }
        }
        testScope.advanceUntilIdle()
        val writer = async {
            lock.withWriteLock("bar") {
                true
            }
        }
        testScope.advanceUntilIdle()
        assertThat(writer.isCompleted).isTrue()
        finishReader.complete(Unit)
        testScope.advanceUntilIdle()
        assertThat(reader.await()).isTrue()
    }
}