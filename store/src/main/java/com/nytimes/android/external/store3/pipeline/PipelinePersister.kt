package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.switchMap

@FlowPreview
class PipelinePersister<Key, Input, Output>(
        private val fetcher: PipelineStore<Key, Input>,
        private val reader: (Key) -> Flow<Output?>,
        private val writer: suspend (Key, Input) -> Unit,
        private val delete: (suspend (Key) -> Unit)? = null
) : PipelineStore<Key, Output> {
    override suspend fun get(request: StoreRequest<Key>): Output? {
        val value: Output? = if (request.shouldSkipCache(CacheType.DISK)) {
            null
        } else {
            reader(request.key).singleOrNull()
        }
        // skipped cache or cache is null
        val fetcherValue = fetcher.get(request)
        fetcherValue?.let {
            writer(request.key, it)
        }
        return reader(request.key).singleOrNull()
    }

    @Suppress("UNCHECKED_CAST")
    override fun stream(request: StoreRequest<Key>): Flow<Output> {
        if (request.shouldSkipCache(CacheType.DISK)) {
            return fetcher.stream(request)
                .switchMap {
                    writer(request.key, it)
                    reader(request.key)
                }.castNonNull()
        } else {
            return reader(request.key).castNonNull()
        }
    }

    override suspend fun clearMemory() {
        fetcher.clearMemory()
    }

    override suspend fun clear(key: Key) {
        fetcher.clear(key)
        delete?.invoke(key)
    }
}

// TODO figure out why filterNotNull does not make compiler happy
@FlowPreview
@Suppress("UNCHECKED_CAST")
private fun <T1, T2> Flow<T1>.castNonNull(): Flow<T2> {
    val self = this
    return flow {
        self.collect {
            if (it != null) {
                emit(it as T2)
            }
        }
    }
}
