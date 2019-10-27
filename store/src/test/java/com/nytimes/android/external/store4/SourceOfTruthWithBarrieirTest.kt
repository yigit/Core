package com.nytimes.android.external.store4

import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@ExperimentalCoroutinesApi
@RunWith(JUnit4::class)
class SourceOfTruthWithBarrieirTest {
    private val testScope = TestCoroutineScope()
    private val delegate: SourceOfTruth<Int, String, String> = InMemorySourceOfTruth()
    private val source = SourceOfTruthWithBarrier(
        delegate = delegate
    )
    @Test
    fun simple() = testScope.runBlockingTest {
        val collector = async {
            source.reader(1).take(2).toList()
        }
        source.write(1, "a")
        Assertions.assertThat(collector.await()).isEqualTo(listOf(
            DataWithOrigin(ResponseOrigin.Fetcher, null),
            DataWithOrigin(ResponseOrigin.Fetcher, "a")
        ))
    }
}