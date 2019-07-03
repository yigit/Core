package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.pipeline.beginPipeline
import com.nytimes.android.external.store3.pipeline.open
import junit.framework.Assert.fail
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized


@RunWith(Parameterized::class)
class DontCacheErrorsTest(
        @Suppress("UNUSED_PARAMETER") name : String,
        buildStore : (suspend (BarCode) -> Int) -> Store<Int, BarCode>
) {

    private var shouldThrow: Boolean = false
    private val store = buildStore {
        if (shouldThrow) {
            throw RuntimeException()
        } else {
            0
        }
    }

    @Test
    fun testStoreDoesntCacheErrors() = runBlocking<Unit> {
        val barcode = BarCode("bar", "code")

        shouldThrow = true

        try {
            store.get(barcode)
            fail()
        } catch (e: RuntimeException) {
            e.printStackTrace()
        }

        shouldThrow = false
        store.get(barcode)
    }

    @FlowPreview
    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun params() = ParamsHelper.withFetcher()
    }
}
