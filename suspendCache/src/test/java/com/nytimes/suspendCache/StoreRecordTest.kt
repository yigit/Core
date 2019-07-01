package com.nytimes.suspendCache

import com.com.nytimes.suspendCache.StoreRecordLoader
import com.com.nytimes.suspendCache.StoreRecordPrecomputed
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@ExperimentalCoroutinesApi
@RunWith(JUnit4::class)
class StoreRecordTest {
    private val testScope = TestCoroutineScope()
    @Test
    fun precomputed() = testScope.runBlockingTest {
        val record = StoreRecordPrecomputed<String, String>("bar")
        assertThat(record.cachedValue()).isEqualTo("bar")
        assertThat(record.value()).isEqualTo("bar")
    }

    @Test
    fun fetched() = testScope.runBlockingTest {
        val record = StoreRecordLoader("foo") { key ->
            assertThat(key).isEqualTo("foo")
            "bar"
        }
        assertThat(record.value()).isEqualTo("bar")
    }

    @Test
    fun fetched_multipleValueGet() = testScope.runBlockingTest {
        var runCount = 0
        val record = StoreRecordLoader("foo") { _ ->
            runCount++
            "bar"
        }
        assertThat(record.value()).isEqualTo("bar")
        assertThat(record.value()).isEqualTo("bar")
        assertThat(runCount).isEqualTo(1)
    }

    @Test
    fun fetched_multipleValueGet_firstError() = testScope.runBlockingTest {
        var runCount = 0
        val errorMsg = "i'd like to fail"
        val record = StoreRecordLoader("foo") { _ ->
            runCount++
            if (runCount == 1) {

                throw RuntimeException(errorMsg)
            } else {
                "bar"
            }
        }
        val first = runCatching {
            record.value()
        }
        assertThat(first.isFailure).isTrue()
        assertThat(first.exceptionOrNull()?.localizedMessage).isEqualTo(errorMsg)
        assertThat(record.value()).isEqualTo("bar")
        assertThat(runCount).isEqualTo(2)
    }

    @Test
    fun fetched_multipleValueGet_firstOneFails_delayed() = testScope.runBlockingTest {
        var runCount = 0
        val firstResponse = CompletableDeferred<String>()
        val secondResponse = CompletableDeferred<String>()
        val errorMsg = "i'd like to fail"
        val record = StoreRecordLoader("foo") { _ ->
            runCount++
            if (runCount == 1) {
                return@StoreRecordLoader firstResponse.await()
            } else {
                return@StoreRecordLoader secondResponse.await()
            }
        }
        val first = async {
            record.value()
        }
        val second = async {
            record.value()
        }
        testScope.advanceUntilIdle()
        assertThat(first.isCompleted).isFalse()
        assertThat(second.isCompleted).isFalse()
        firstResponse.completeExceptionally(RuntimeException(errorMsg))

        assertThat(first.isCompleted).isTrue()
        assertThat(second.isCompleted).isFalse()

        assertThat(first.getCompletionExceptionOrNull()?.localizedMessage).isEqualTo(errorMsg)

        secondResponse.complete("bar")
        assertThat(second.await()).isEqualTo("bar")
        assertThat(runCount).isEqualTo(2)
    }
}