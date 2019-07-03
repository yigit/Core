package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.Clearable
import com.nytimes.android.external.store3.base.Fetcher
import com.nytimes.android.external.store3.base.Parser
import com.nytimes.android.external.store3.base.Persister
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.base.wrappers.cache
import com.nytimes.android.external.store3.base.wrappers.parser
import com.nytimes.android.external.store3.base.wrappers.persister
import com.nytimes.android.external.store3.pipeline.*
import com.nytimes.android.external.store3.util.KeyParser
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow

data class TestStoreBuilder<Key, Output>(
        private val buildStore: () -> Store<Output, Key>,
        private val buildPipelineStore: () -> Store<Output, Key>
) {
    fun build(storeType: TestStoreType): Store<Output, Key> = when (storeType) {
        TestStoreType.Store -> buildStore()
        TestStoreType.Pipeline -> buildPipelineStore()
    }

    companion object {
        @FlowPreview
        fun <Key, Output> from(
                inflight: Boolean = true,
                fetcher: suspend (Key) -> Output
        ): TestStoreBuilder<Key, Output> = from(
                inflight = inflight,
                persister = null,
                keyParser = null,
                fetcher = fetcher
        )

        @FlowPreview
        fun <Key, Output> from(
                inflight: Boolean = true,
                fetcher: suspend (Key) -> Output,
                parser : KeyParser<Key, Output, Output>
        ): TestStoreBuilder<Key, Output> = from(
                inflight = inflight,
                persister = null,
                keyParser = parser,
                fetcher = fetcher
        )

        @FlowPreview
        fun <Key, Output> from(
                inflight: Boolean = true,
                fetcher: Fetcher<Output, Key>,
                parser : Parser<Output, Output>,
                persister: Persister<Output, Key>
        ): TestStoreBuilder<Key, Output> = from(
                inflight = inflight,
                persister = persister,
                keyParser = object : KeyParser<Key, Output, Output> {
                    override suspend fun apply(key: Key, raw: Output): Output {
                        return parser.apply(raw)
                    }
                },
                fetcher = fetcher
        )

        @Suppress("UNCHECKED_CAST")
        @FlowPreview
        fun <Key, Output> from(
                inflight: Boolean = true,
                cached : Boolean = false,
                cacheMemoryPolicy: MemoryPolicy? = null,
                persister: Persister<Output, Key>? = null,
                keyParser: KeyParser<Key, Output, Output>? = null,
                fetcher: suspend (Key) -> Output
        ): TestStoreBuilder<Key, Output> = from(
                inflight = inflight,
                cached = cached,
                cacheMemoryPolicy = cacheMemoryPolicy,
                persister = persister,
                keyParser = keyParser,
                fetcher = object : Fetcher<Output, Key> {
                    override suspend fun fetch(key: Key): Output = fetcher(key)
                }
        )
        @Suppress("UNCHECKED_CAST")
        @FlowPreview
        fun <Key, Output> from(
                inflight: Boolean = true,
                cached : Boolean = false,
                cacheMemoryPolicy: MemoryPolicy? = null,
                persister: Persister<Output, Key>? = null,
                keyParser: KeyParser<Key, Output, Output>? = null,
                fetcher: Fetcher<Output, Key>
        ): TestStoreBuilder<Key, Output> {
            return TestStoreBuilder(
                    buildStore = {
                        Store.from(
                                inflight = inflight,
                                f = fetcher
                        ).let {
                            if (keyParser == null) {
                                it
                            } else {
                                it.parser(keyParser)
                            }
                        }.let {
                            if (persister == null) {
                                it
                            } else {
                                it.persister(persister)
                            }
                        }.let {
                            if (cached) {
                                it.cache(cacheMemoryPolicy)
                            } else {
                                it
                            }
                        }.open()
                    },
                    buildPipelineStore = {
                        beginPipeline<Key, Output>(
                                fetcher = {
                                    flow {
                                        emit(fetcher.fetch(it))
                                    }
                                }
                        ).let {
                            if (keyParser == null) {
                                it
                            } else {
                                it.withKeyConverter { key, oldOutput ->
                                    keyParser.apply(key, oldOutput)
                                }
                            }
                        }.let {
                            if (persister == null) {
                                it
                            } else {
                                it.withPersister(
                                        reader = { key: Key ->
                                            flow {
                                                persister.read(key)?.let { value: Output ->
                                                    emit(value)
                                                }
                                            }
                                        },
                                        writer = { key, value ->
                                            persister.write(key, value)
                                        },
                                        delete = if (persister is Clearable<*>) {
                                            SuspendWrapper(
                                                    (persister as Clearable<Key>)::clear
                                            )::apply
                                        } else {
                                            null
                                        }
                                )
                            }
                        }.let {
                            if (cached) {
                                it.withCache(cacheMemoryPolicy)
                            } else {
                                it
                            }
                        }.open()
                    }
            )
        }
    }

    // wraps a regular fun to suspend, couldn't figure out how to create suspend fun variables :/
    private class SuspendWrapper<P0, R>(
            val f : (P0) -> R
    ) {
        suspend fun apply(input : P0) : R = f(input)
    }
}

enum class TestStoreType {
    Store,
    Pipeline
}