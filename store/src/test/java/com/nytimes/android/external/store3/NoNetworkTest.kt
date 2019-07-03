package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.base.impl.Store
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class NoNetworkTest(
        @Suppress("UNUSED_PARAMETER") name : String,
        buildStore : (suspend (BarCode) -> Any) -> Store<Any, BarCode>
) {
    private val store: Store<Any, BarCode> = buildStore {
        throw EXCEPTION
    }

    @Test
    fun testNoNetwork() = runBlocking<Unit> {
        try {
            store.get(BarCode("test", "test"))
            fail("Exception not thrown")
        } catch (e: Exception) {
            assertThat(e.message).isEqualTo(EXCEPTION.message)
        }
    }

    @FlowPreview
    companion object {
        private val EXCEPTION = RuntimeException("abc")

        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun params() = ParamsHelper.withFetcher<BarCode, Int>(cached = false)
    }
}
