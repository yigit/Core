package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.base.wrappers.parser
import com.nytimes.android.external.store3.pipeline.beginPipeline
import com.nytimes.android.external.store3.pipeline.open
import com.nytimes.android.external.store3.pipeline.withKeyConverter
import com.nytimes.android.external.store3.util.KeyParser
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow

/**
 * Utility class that avoid duplication among parameterized tests which test existing store impl
 * and the Pipeline implementation
 */
object ParamsHelper {
    val CONTROL = "control"
    val PIPELINE = "pipeline"
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
                arrayOf(CONTROL, controlStore),
                arrayOf(PIPELINE, pipelineStore))
    }

    @FlowPreview
    fun <Key, OldOutput, NewOutput>withFetcherAndKeyParser(): List<Array<Any>> {
        val controlStore = fun(
                fetcher : suspend (Key) -> OldOutput,
                parser : KeyParser<Key, OldOutput, NewOutput>) : Store<NewOutput, Key> {
            return Store.from(
                    inflight = true,
                    f = fetcher)
                    .parser(parser)
                    .open()
        }

        val pipelineStore =  fun(
                fetcher : suspend (Key) -> OldOutput,
                parser : KeyParser<Key, OldOutput, NewOutput>) : Store<NewOutput, Key> {
            return beginPipeline<Key, OldOutput> {
                flow {
                    emit(fetcher(it))
                }
            }.withKeyConverter { key, oldOutput ->
                parser.apply(key, oldOutput)
            }.open()
        }
        return listOf(
                arrayOf(CONTROL, controlStore),
                arrayOf(PIPELINE, pipelineStore))
    }
}