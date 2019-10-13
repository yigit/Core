package com.nytimes.android.external.store3.pipeline

import com.nytimes.android.external.store3.pipeline.ResponseOrigin.Cache
import com.nytimes.android.external.store3.pipeline.ResponseOrigin.Fetcher
import com.nytimes.android.external.store3.pipeline.ResponseOrigin.Persister
import com.nytimes.android.external.store3.pipeline.StoreResponse.Data
import com.nytimes.android.external.store3.pipeline.StoreResponse.Loading
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.flow.retryWhen
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
import java.io.IOException

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
            .assertCompleteStream(
                Loading(
                    origin = Fetcher
                ), Data(
                    value = "three-1",
                    origin = Fetcher
                )
            )
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertCompleteStream(
                Data(
                    value = "three-1",
                    origin = Cache
                )
            )
        pipeline.stream(StoreRequest.fresh(3))
            .assertItems(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(
                Data(
                    value = "three-2",
                    origin = Cache
                )
            )
    }

    @Test
    fun deleteme() {
        flowOf("a", "B", "c")
            .retryWhen { cause, attempt ->
                if (attempt < 10 && cause is IOException) {
                    emit("error: ${cause.message}")
                    delay(1_000)
                    true
                } else {
                    emit("cannot get value")
                    false
                }
            }
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
            .assertItems(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-1",
                    origin = Fetcher
                )
            )
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(
                Data(
                    value = "three-1",
                    origin = Cache
                )
            )
        pipeline.stream(StoreRequest.fresh(3))
            .assertItems(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )
        pipeline.stream(StoreRequest.cached(3, refresh = false))
            .assertItems(
                Data(
                    value = "three-2",
                    origin = Cache
                )
            )
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
            .assertItems(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-1",
                    origin = Fetcher
                )
            )

        pipeline.stream(StoreRequest.cached(3, refresh = true))
            .assertItems(
                Data(
                    value = "three-1",
                    origin = Cache
                ),
                Data(
                    value = "three-1",
                    origin = Persister
                ),
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )
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
            .assertCompleteStream(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-1",
                    origin = Fetcher
                )
            )

        pipeline.stream(StoreRequest.cached(3, refresh = true))
            .assertCompleteStream(
                Data(
                    value = "three-1",
                    origin = Cache
                ),
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )
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
            .assertCompleteStream(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-1",
                    origin = Fetcher
                )
            )

        pipeline.stream(StoreRequest.skipMemory(3, refresh = false))
            .assertCompleteStream(
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )
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
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "three-1",
                    origin = Fetcher
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )

        pipeline.stream(StoreRequest.cached(3, refresh = true)).assertItems(
            Data(
                value = "three-2",
                origin = Persister
            ),
            Loading(
                origin = Fetcher
            ),
            Data(
                value = "three-1",
                origin = Fetcher
            ),
            Data(
                value = "three-2",
                origin = Fetcher
            )
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
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "local-1",
                    origin = Persister
                )
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
                Loading(
                    origin = Fetcher
                ),
                Data(
                    value = "local-1",
                    origin = Persister
                ),
                Data(
                    value = "three-1",
                    origin = Fetcher
                ),
                Data(
                    value = "local-2",
                    origin = Persister
                ),
                Data(
                    value = "three-2",
                    origin = Fetcher
                )
            )
    }

    @Test
    fun errorTest() = testScope.runBlockingTest {
        val exception = IllegalArgumentException("wow")
        val persister = InMemoryPersister<Int, String>().asObservable()
        val pipeline = beginNonFlowingPipeline<Int, String> { key: Int ->
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
                Loading(
                    origin = Fetcher
                ),
                StoreResponse.Error(
                    error = exception,
                    origin = Fetcher
                ),
                Data(
                    value = "local-1",
                    origin = Persister
                )
            )
        pipeline.stream(StoreRequest.cached(key = 3, refresh = true))
            .assertItems(
                Data(
                    value = "local-1",
                    origin = Persister
                ),
                Loading(
                    origin = Fetcher
                ),
                StoreResponse.Error(
                    error = exception,
                    origin = Fetcher
                )
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
}