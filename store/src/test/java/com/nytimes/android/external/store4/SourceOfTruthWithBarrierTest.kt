package com.nytimes.android.external.store4

import com.nytimes.android.external.store3.pipeline.PipelineStoreTest
import com.nytimes.android.external.store3.pipeline.ResponseOrigin
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.AssumptionViolatedException
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@FlowPreview
@ExperimentalCoroutinesApi
@RunWith(Parameterized::class)
class SourceOfTruthWithBarrierTest(
    private val persistent: Boolean
) {
    private val testScope = TestCoroutineScope()

    private val delegate: SourceOfTruth<Int, String, String> = if (persistent) {
        val persister = PipelineStoreTest.InMemoryPersister<Int, String>()
        PersistentSourceOfTruth(
            realReader = {key ->
                flow {
                    emit(persister.read(key))
                }
            },
            realWriter = persister::write,
            realDelete = null
        )
    } else {
        InMemorySourceOfTruth()
    }
    private val source = SourceOfTruthWithBarrier(
        delegate = delegate
    )

    @Test
    fun simple() = testScope.runBlockingTest {
        val collector = async {
            source.reader(1, CompletableDeferred(Unit)).take(2).toList()
        }
        source.write(1, "a")
        assertThat(collector.await()).isEqualTo(listOf(
            DataWithOrigin(delegate.defaultOrigin, null),
            DataWithOrigin(ResponseOrigin.Fetcher, "a")
        ))
    }

    @Test
    fun preAndPostWrites() = testScope.runBlockingTest {
        if (!persistent) {
            throw AssumptionViolatedException("only for persistent")
        }
        source.write(1, "a")
        val collector = async {
            source.reader(1, CompletableDeferred(Unit)).take(2).toList()
        }
        source.write(1, "b")
        assertThat(collector.await()).isEqualTo(listOf(
            DataWithOrigin(delegate.defaultOrigin, "a"),
            DataWithOrigin(ResponseOrigin.Fetcher, "b")
        ))
    }

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "persistent={0}")
        fun params() = listOf(true, false)
    }
}