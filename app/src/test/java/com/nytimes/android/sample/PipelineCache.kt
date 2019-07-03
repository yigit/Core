package com.nytimes.android.sample

import com.com.nytimes.suspendCache.StoreCache
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.StoreDefaults
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*

@FlowPreview
internal class CachePipeline<Output, Key>(
        private val delegate: ReaderPipeline<Key, Output>,
        memoryPolicy: MemoryPolicy?
) : ReaderMemoryCachePipeline<Key, Output> {
    @Suppress("UNCHECKED_CAST")
    private val memCache = StoreCache.from(
            loader = { key: Key ->
                delegate.stream(key).single()
            },
            memoryPolicy = memoryPolicy ?: StoreDefaults.memoryPolicy
    )

    override suspend fun stream(key: Key): Flow<Output> {
        return delegate.stream(key).onEach {
            memCache.put(key, it)
        }
    }

    override suspend fun clearMemory(key: Key) {
        memCache.invalidate(key)
    }

    override suspend fun clearAllMemory() {
        memCache.clearAll()
    }

    companion object {
        fun <Key, Output> from(
        reader : ReaderPipeline<Key, Output>,
        memoryPolicy: MemoryPolicy?
        )  = CachePipeline(reader, memoryPolicy)
    }
}