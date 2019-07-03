package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.pipeline.beginPipeline
import com.nytimes.android.external.store3.pipeline.open
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class ClearStoreMemoryTest(
        @Suppress("UNUSED_PARAMETER") name : String,
        buildStore : (suspend (BarCode) -> Int) -> Store<Int, BarCode>
) {
    private var networkCalls = 0
    private val store = buildStore {
        networkCalls++
    }

    @Test
    fun testClearSingleBarCode() = runBlocking<Unit> {
        //one request should produce one call
        val barcode = BarCode("type", "key")
        store.get(barcode)
        assertThat(networkCalls).isEqualTo(1)

        // after clearing the memory another call should be made
        store.clear(barcode)
        store.get(barcode)
        assertThat(networkCalls).isEqualTo(2)
    }

    @Test
    fun testClearAllBarCodes() = runBlocking<Unit> {
        val b1 = BarCode("type1", "key1")
        val b2 = BarCode("type2", "key2")

        //each request should produce one call
        store.get(b1)
        store.get(b2)
        assertThat(networkCalls).isEqualTo(2)

        store.clear(b1)
        store.clear(b2)

        //after everything is cleared each request should produce another 2 calls
        store.get(b1)
        store.get(b2)
        assertThat(networkCalls).isEqualTo(4)
    }

    @FlowPreview
    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun params() = ParamsHelper.withFetcher<BarCode, Int>(cached = false)
    }
}
