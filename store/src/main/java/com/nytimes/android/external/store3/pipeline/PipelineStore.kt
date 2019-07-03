package com.nytimes.android.external.store3.pipeline

import com.nytimes.android.external.store3.base.impl.Store
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking

// taken from
// https://github.com/Kotlin/kotlinx.coroutines/blob/7699a20982c83d652150391b39567de4833d4253/kotlinx-coroutines-core/js/src/flow/internal/FlowExceptions.kt
internal class AbortFlowException :  CancellationException("Flow was aborted, no more elements needed")
// possible replacement for [Store] as an internal only representation
// if this class becomes public, should probaly be named IntermediateStore to distingush from
// Store and also clarify that it still needs to be built/open? (how do we ensure?)
@FlowPreview
interface PipelineStore<Key, Input, Output> {
    /**
     * Return a flow for the given key
     */
    suspend fun stream(key: Key): Flow<Output>

    /**
     * Return a flow for the given key and skip all cache
     */
    suspend fun streamFresh(key: Key): Flow<Output>

    /**
     * Return a single value for the given key.
     */
    suspend fun get(key : Key) : Output? {
        var result : Output? = null
        try {
            stream(key).collect {
                result = it
                throw AbortFlowException()
            }
        } catch (abort : AbortFlowException) {
        }
        return result
    }

    /**
     * Return a single value for the given key.
     */
    suspend fun fresh(key : Key) : Output? {
        var result : Output? = null
        try {
            streamFresh(key).collect {
                result = it
                throw AbortFlowException()
            }
        } catch (abort : AbortFlowException) {
        }
        return result
    }


    /**
     * Clear the memory cache of all entries
     */
    suspend fun clearMemory()

    /**
     * Purge a particular entry from memory and disk cache.
     * Persister will only be cleared if they implements Clearable
     */
    suspend fun clear(key: Key)
}

@FlowPreview
fun <Key, Input, Output> PipelineStore<Key, Input, Output>.open() : Store<Output, Key> {
    val self = this
    return object : Store<Output, Key> {
        override suspend fun get(key: Key) = self.get(key)!!

        override suspend fun fresh(key: Key) = self.fresh(key)!!

        @FlowPreview
        override fun stream(): Flow<Pair<Key, Output>> = TODO("not supported"

        )

        @FlowPreview
        override fun stream(key: Key) = runBlocking {
            self.stream(key)
        }

        override suspend fun clearMemory() {
            runBlocking {
                self.clearMemory()
            }
        }

        override suspend fun clear(key: Key) = self.clear(key)

    }
}