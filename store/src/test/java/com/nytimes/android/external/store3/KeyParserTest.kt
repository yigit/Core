package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.util.KeyParser
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@FlowPreview
@RunWith(Parameterized::class)
class KeyParserTest(
        @Suppress("UNUSED_PARAMETER") name: String,
        buildStore: (suspend (Int) -> String, KeyParser<Int, String, String>) -> Store<String, Int>
) {
    private val store = buildStore(
            { NETWORK },
            object : KeyParser<Int, String, String> {
                override suspend fun apply(key: Int, raw: String): String {
                    return raw + key
                }
            }
    )

    @Test
    fun testStoreWithKeyParserFuncNoPersister() = runBlocking<Unit> {
        assertThat(store.get(KEY)).isEqualTo(NETWORK + KEY)
    }

    companion object {
        private const val NETWORK = "Network"
        private const val KEY = 5
        @JvmStatic
        @Parameterized.Parameters(name = "{0}")
        fun params() = ParamsHelper.withFetcherAndKeyParser<Int, String, String>()
    }
}
