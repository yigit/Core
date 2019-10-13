package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class StoreRequestKeyManagerTest {
    private val testScope = TestCoroutineScope()

    @Test
    fun testMultiplexed() = testScope.runBlockingTest {
        if (true) return@runBlockingTest
        val fetcher = FakeFetcher(
            3 to "three-1"
        )
        val pipeline = beginNonFlowingPipeline(fetcher::fetch)
            .withCache()
        val keyManager = StoreRequestKeyManager(
            pipeline = pipeline,
            key = 3,
            scope = testScope)
        val flow1 = keyManager.stream(StoreRequest.cached(3, true))
        val flow2 = keyManager.stream(StoreRequest.cached(3, true))
        flow1.assertItems(
            StoreResponse.Loading(origin = ResponseOrigin.Fetcher),
            StoreResponse.Data(origin = ResponseOrigin.Fetcher, value = "three-1")
        )
        flow2.assertItems(
            StoreResponse.Loading(origin = ResponseOrigin.Fetcher),
            StoreResponse.Data(origin = ResponseOrigin.Fetcher, value = "three-1")
        )
    }
}