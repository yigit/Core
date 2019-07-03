package com.nytimes.android.external.store3.pipeline

import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single
import kotlin.experimental.ExperimentalTypeInference

// used when there is no input
class NoInput

@FlowPreview
class Pipeline<Key, Input, Output> constructor(
        val reader : ReadPipeline<Key, Output>?,
        val writer: WritePipeline<Key, Input>?
) : PipelineStore<Key, Input, Output> {
    override suspend fun streamFresh(key: Key): Flow<Output> {
        return stream(key)
    }

    override suspend fun stream(key: Key) = reader?.read(key) ?: throw UnsupportedOperationException("")

    override suspend fun fresh(key: Key): Output  = streamFresh(key).single()

    override suspend fun clearMemory() {
    }

    override suspend fun clear(key: Key) {
    }
}

// just a marker to say something is cached
interface Cached<Key>

// similar to DiskRead
interface WritePipeline<Key, Input> {
    fun write(key : Key, input : Input)
}
// similar to DiskWrite
@FlowPreview
interface ReadPipeline<Key, Output> {
    fun read(key : Key) : Flow<Output>
}

@FlowPreview
fun <Key, Output> beginPipeline(
        fetcher : suspend (Key) -> Flow<Output>
) : PipelineStore<Key, NoInput, Output> {
    return PipelineFetcherStore(fetcher)
}

@FlowPreview
fun <Key, Input, OldOutput, NewOutput>PipelineStore<Key, Input, OldOutput>.withConverter(
        converter : suspend (OldOutput) -> NewOutput
) : PipelineStore<Key, Input, NewOutput> {
    return PipelineConverterStore(this, converter)
}

@FlowPreview
fun <Key, Input, Output>PipelineStore<Key, Input, Output>.withCache(
        memoryPolicy: MemoryPolicy? = null
) : PipelineStore<Key, Input, Output> {
    return PipelineCacheStore(this, memoryPolicy)
}

@FlowPreview
fun <Key, OldInput, OldOutput, NewOutput>PipelineStore<Key, OldInput, OldOutput>.withPersister(
        reader : suspend (Key) -> Flow<NewOutput>,
        writer : suspend (Key, OldOutput) -> Unit,
        delete : (suspend (Key) -> Unit)? = null
) : PipelineStore<Key, OldOutput, NewOutput> {
    return PipelinePersister(
            fetcher = this,
            reader = reader,
            writer = writer,
            delete = delete
    )
}