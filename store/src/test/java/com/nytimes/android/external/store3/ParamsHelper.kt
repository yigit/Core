package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.Fetcher
import com.nytimes.android.external.store3.base.Parser
import com.nytimes.android.external.store3.base.Persister
import com.nytimes.android.external.store3.base.impl.ParsingFetcher
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.base.wrappers.cache
import com.nytimes.android.external.store3.base.wrappers.parser
import com.nytimes.android.external.store3.base.wrappers.persister
import com.nytimes.android.external.store3.pipeline.*
import com.nytimes.android.external.store3.util.KeyParser
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow

/**
 * Utility class that avoid duplication among parameterized tests which test existing store impl
 * and the Pipeline implementation
 */
@FlowPreview
object ParamsHelper {
    val CONTROL = "control"
    val PIPELINE = "pipeline"

    fun <Key, Output> withFetcher(cached: Boolean): List<Array<Any>> {
        val controlStore = fun(fetcher: suspend (Key) -> Output): Store<Output, Key> {
            val builder = Store.from(
                    inflight = true,
                    f = fetcher)
            return if (cached) {
                builder.cache()
            } else {
                builder
            }.open()
        }

        val pipelineStore = fun(fetcher: suspend (Key) -> Output): Store<Output, Key> {
            val pipeline = beginPipeline<Key, Output> {
                flow {
                    emit(fetcher(it))
                }
            }
            return if (cached) {
                pipeline.withCache()
            } else {
                pipeline
            }.open()
        }
        return listOf(
                arrayOf(CONTROL, controlStore),
                arrayOf(PIPELINE, pipelineStore))
    }

    fun <Key, OldOutput, NewOutput> withFetcherAndKeyParser(): List<Array<Any>> {
        val controlStore = fun(
                fetcher: suspend (Key) -> OldOutput,
                parser: KeyParser<Key, OldOutput, NewOutput>): Store<NewOutput, Key> {
            return Store.from(
                    inflight = true,
                    f = fetcher)
                    .parser(parser)
                    .open()
        }

        val pipelineStore = fun(
                fetcher: suspend (Key) -> OldOutput,
                parser: KeyParser<Key, OldOutput, NewOutput>): Store<NewOutput, Key> {
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

    fun <Key, Output> withParserFetcherPersister(): List<Array<Any>> {
        val controlStore = fun(
                fetcher: Fetcher<Output, Key>,
                parser: Parser<Output, Output>,
                persister: Persister<Output, Key>
        ): Store<Output, Key> {
            return Store.from(ParsingFetcher.from(fetcher, parser))
                    .persister(persister)
                    .open()
        }
        val pipelineStore = fun(
                fetcher: Fetcher<Output, Key>,
                parser: Parser<Output, Output>,
                persister: Persister<Output, Key>
        ): Store<Output, Key> {
            return beginPipeline<Key, Output> {
                flow {
                    emit(fetcher.fetch(it))
                }
            }.withConverter {
                parser.apply(it)
            }.withPersister(
                    reader = {
                        flow {
                            persister.read(key = it)?.let {
                                emit(it)
                            }
                        }
                    },
                    writer = { key, value ->
                        persister.write(key, value)
                    }
            ).open() as Store<Output, Key>
        }
        return listOf(
                arrayOf(CONTROL, controlStore),
                arrayOf(PIPELINE, pipelineStore))
    }
}