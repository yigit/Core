package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.pipeline.beginPipeline
import com.nytimes.android.external.store3.pipeline.open
import com.nytimes.android.external.store3.util.KeyParser
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow

/**
 * Utility class that avoid duplication among parameterized tests which test existing store impl
 * and the Pipeline implementation
 */
object ParamsHelper {
    @FlowPreview
    fun <Key, Output>withFetcher(): List<Array<Any>> {
        val controlStore = fun(fetcher : suspend (Key) -> Output) : Store<Output, Key> {
            return Store.from(
                    inflight = true,
                    f = fetcher).open()
        }

        val pipelineStore = fun(fetcher : suspend (Key) -> Output) : Store<Output, Key> {
            return beginPipeline<Key, Output> {
                flow {
                    emit(fetcher(it))
                }
            }.open()
        }
        return listOf(
                arrayOf("control", controlStore),
                arrayOf("pipeline", pipelineStore))
    }
}