package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.switchMap
import kotlinx.coroutines.launch

@FlowPreview
class PipelinePersister<Key, Input, Output>(
    private val fetcher: PipelineStore<Key, *, Input>,
    private val reader: (Key) -> Flow<Output?>,
    private val writer: suspend (Key, Input) -> Unit,
    private val delete: (suspend (Key) -> Unit)? = null
) : PipelineStore<Key, Input, Output> {
    override suspend fun get(key: Key): Output? {
        val value: Output? = reader(key).singleOrNull()
        value?.let {
            // cached value from persister
            return it
        }
        // nothing is cached, get fetcher
        val fetcherValue = fetcher.get(key)
            ?: return null // no fetch, no result
        writer(key, fetcherValue)
        return reader(key).singleOrNull()
    }

    override suspend fun fresh(key: Key): Output? {
        // nothing is cached, get fetcher
        val fetcherValue = fetcher.fresh(key)
            ?: return null // no fetch, no result TODO should we invalidate cache, probably not?
        writer(key, fetcherValue)
        return reader(key).singleOrNull()
    }

    @Suppress("UNCHECKED_CAST")
    override fun stream(key: Key): Flow<Output> {
        return reader(key)
            // TODO: should we really call fetcher.streamFresh ? maybe let developer specify
            // via StoreCall ?
            // the assumption is that we always want to update from backend but what if we
            // don't. Should we instead call just stream? But if it is cached, we are basically
            // re-writing dummy data back because we don't know :/
            .sideCollect(fetcher.streamFresh(key)) {
                writer(key, it)
            }.castNonNull()
    }

    @Suppress("UNCHECKED_CAST")
    override fun streamFresh(key: Key): Flow<Output> {
        return fetcher.streamFresh(key)
            .switchMap {
                writer(key, it)
                reader(key)
            }.castNonNull()
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

@UseExperimental(FlowPreview::class)
private fun <T, R> Flow<T>.sideCollect(
    other: Flow<R>,
    otherCollect: suspend (R) -> Unit
) = flow {
    coroutineScope {
        launch {
            other.collect {
                otherCollect(it)
            }
        }
        this@sideCollect.collect {
            emit(it)
        }
    }
}