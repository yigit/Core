package com.nytimes.android.external.store3.pipeline

import com.nytimes.android.external.store3.pipeline.StoreResponse.Loading
import com.nytimes.android.external.store3.pipeline.StoreResponse.Success
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@ExperimentalCoroutinesApi
@RunWith(JUnit4::class)
class PipelineStoreTest {
    private val testScope = TestCoroutineScope()

    @Test
    fun getAndFresh() = testScope.runBlockingTest {
        val fetcher = FakeFetcher(
            3 to "three-1",
            3 to "three-2"
        )
        val pipeline = beginNonFlowingPipeline(fetcher::fetch)
            .withCache()
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(Loading(), Success("three-1"))
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(Success("three-1"))
        pipeline.stream(StoreRequest.fresh(3))
            .assertItems(Loading(), Success("three-2"))
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(Success("three-2"))
    }

    @Test
    fun getAndFresh_withPersister() = runBlocking<Unit> {
        val fetcher = FakeFetcher(
            3 to "three-1",
            3 to "three-2"
        )
        val persister = InMemoryPersister<Int, String>()
        val pipeline = beginNonFlowingPipeline(fetcher::fetch)
            .withNonFlowPersister(
                reader = persister::read,
                writer = persister::write
            )
            .withCache()
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(Loading(), Success("three-1"))
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(Success("three-1"))

        pipeline.stream(StoreRequest.fresh(3))
            .assertItems(Loading(), Success("three-2"))
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(Success("three-2"))
    }

    @Test
    fun streamAndFresh_withPersister() = testScope.runBlockingTest {
        val fetcher = FakeFetcher(
            3 to "three-1",
            3 to "three-2"
        )
        val persister = InMemoryPersister<Int, String>()

        val pipeline = beginNonFlowingPipeline(fetcher::fetch)
            .withNonFlowPersister(
                reader = persister::read,
                writer = persister::write
            )
            .withCache()

        pipeline.stream(StoreRequest.cached(3, refresh = true))
            .assertItems(Loading(), Success("three-1"))

        pipeline.stream(StoreRequest.cached(3, refresh = true))
            .assertItems(Loading("three-1"), Success("three-2"))
    }

    @Test
    fun streamAndFresh() = testScope.runBlockingTest {
        val fetcher = FakeFetcher(
            3 to "three-1",
            3 to "three-2"
        )
        val pipeline = beginNonFlowingPipeline(fetcher::fetch)
            .withCache()

        pipeline.stream(StoreRequest.cached(3, refresh = true))
            .assertItems(Loading(), Success("three-1"))

        pipeline.stream(StoreRequest.cached(3, refresh = true))
            .assertItems(Loading("three-1"), Success("three-2"))
    }

    @Test
    fun skipCache() = testScope.runBlockingTest {
        val fetcher = FakeFetcher(
            3 to "three-1",
            3 to "three-2"
        )
        val pipeline = beginNonFlowingPipeline(fetcher::fetch)
            .withCache()

        pipeline.stream(StoreRequest.skipMemory(3, refresh = false))
            .assertItems(Loading(), Success("three-1"))

        pipeline.stream(StoreRequest.skipMemory(3, refresh = false))
            .assertItems(Loading(), Success("three-2"))
    }

    @Test
    fun flowingFetcher() = testScope.runBlockingTest {
        val fetcher = FlowingFakeFetcher(
            3 to "three-1",
            3 to "three-2"
        )
        val persister = InMemoryPersister<Int, String>()

        val pipeline = beginPipeline(fetcher::createFlow)
            .withNonFlowPersister(
                reader = persister::read,
                writer = persister::write
            )
        pipeline.stream(StoreRequest.fresh(3))
            .assertItems(
                Loading(),
                Success("three-1"),
                Success("three-2")
            )
        pipeline.stream(StoreRequest.fresh(3))

        pipeline.stream(StoreRequest.cached(3, refresh = true)).assertItems(
            Loading("three-2"),
            Success("three-1"),
            Success("three-2")
        )
    }

    @Test
    fun diskChangeWhileNetworkIsFlowing_simple() = testScope.runBlockingTest {
        val persister = InMemoryPersister<Int, String>().asObservable()
        val pipeline = beginPipeline<Int, String> {
            flow {
                // never emit
            }
        }.withPersister(
            reader = persister::flowReader,
            writer = persister::flowWriter
        )
        launch {
            delay(10)
            persister.flowWriter(3, "local-1")
        }
        pipeline
            .stream(StoreRequest.cached(3, refresh = true))
            .assertItems(
                Loading(),
                Loading("local-1")
            )
    }

    @Test
    fun diskChangeWhileNetworkIsFlowing_overwrite() = testScope.runBlockingTest {
        val persister = InMemoryPersister<Int, String>().asObservable()
        val pipeline = beginPipeline<Int, String> {
            flow {
                delay(10)
                emit("three-1")
                delay(10)
                emit("three-2")
            }
        }.withPersister(
            reader = persister::flowReader,
            writer = persister::flowWriter
        )
        launch {
            delay(5)
            persister.flowWriter(3, "local-1")
            delay(10) // go in between two server requests
            persister.flowWriter(3, "local-2")
        }
        pipeline
            .stream(StoreRequest.cached(3, refresh = true))
            .assertItems(
                Loading(),
                Loading("local-1"),
                Success("three-1"),
                Success("local-2"),
                Success("three-2")
            )
    }

    @Test
    fun errorTest() = testScope.runBlockingTest {
        val exception = IllegalArgumentException("wow")
        val persister = InMemoryPersister<Int, String>().asObservable()
        val pipeline = beginNonFlowingPipeline<Int, String> { key : Int ->
            throw exception
        }.withPersister(
            reader = persister::flowReader,
            writer = persister::flowWriter
        )
        launch {
            delay(10)
            persister.flowWriter(3, "local-1")
        }
        pipeline.stream(StoreRequest.cached(key = 3, refresh = true))
            .assertItems(
                Loading(),
                StoreResponse.Error(exception, data = null),
                StoreResponse.Error(exception, data = "local-1")
            )
        pipeline.stream(StoreRequest.cached(key = 3, refresh = true))
            .assertItems(
                Loading("local-1"),
                StoreResponse.Error(exception, data = "local-1")
            )
    }

    suspend fun PipelineStore<Int, String>.get(request: StoreRequest<Int>) =
        this.stream(request).filter { it.dataOrNull() != null }.first()

    suspend fun PipelineStore<Int, String>.get(key: Int) = get(
        StoreRequest.cached(
            key = key,
            refresh = false
        )
    )

    suspend fun PipelineStore<Int, String>.fresh(key: Int) = get(
        StoreRequest.fresh(
            key = key
        )
    )

    private class FlowingFakeFetcher<Key, Output>(
        vararg val responses: Pair<Key, Output>
    ) {
        fun createFlow(key: Key) = flow {
            responses.filter {
                it.first == key
            }.forEach {
                emit(it.second)
                delay(1)
            }
        }
    }

    private class FakeFetcher<Key, Output>(
        vararg val responses: Pair<Key, Output>
    ) {
        private var index = 0
        @Suppress("RedundantSuspendModifier") // needed for function reference
        suspend fun fetch(key: Key): Output {
            if (index >= responses.size) {
                throw AssertionError("unexpected fetch request")
            }
            val pair = responses[index++]
            assertThat(pair.first).isEqualTo(key)
            return pair.second
        }
    }

    private class InMemoryPersister<Key, Output> {
        private val data = mutableMapOf<Key, Output>()

        @Suppress("RedundantSuspendModifier")// for function reference
        suspend fun read(key: Key) = data[key].also {
            println("log read $key to $it")
        }

        @Suppress("RedundantSuspendModifier") // for function reference
        suspend fun write(key: Key, output: Output) {
            println("log write $key to $output")
            data[key] = output
        }

        suspend fun asObservable() = SimplePersisterAsFlowable(
                reader = this::read,
                writer = this::write
        )
    }

    private suspend fun <T> Flow<T>.assertItems(vararg expected : T) {
        assertThat(this.take(expected.size).toList())
            .isEqualTo(expected.toList())
    }
}