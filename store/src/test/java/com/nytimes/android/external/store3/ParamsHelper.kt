package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.pipeline.beginPipeline
import com.nytimes.android.external.store3.pipeline.open
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow
import org.junit.runners.Parameterized

/**
 * Utility class that avoid duplication among parameterized tests which test existing store impl
 * and the Pipeline implementation
 */
object ParamsHelper {
    @FlowPreview
    fun withFetcher(): List<Array<Any>> {
        val controlStore = fun(fetcher : suspend (BarCode) -> Int) : Store<Int, BarCode> {
            return Store.from(
                    inflight = true,
                    f = fetcher).open()
        }

        val pipelineStore = fun(fetcher : suspend (BarCode) -> Int) : Store<Int, BarCode> {
            return beginPipeline<BarCode, Int> {
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