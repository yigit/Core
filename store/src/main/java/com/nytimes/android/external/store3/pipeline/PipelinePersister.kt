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
    override suspend fun get(key: Key): StoreResponse<Output> {
        val value: Output? = reader(key).singleOrNull()
        value?.let {
            // cached value from persister
            return StoreResponse.SuccessResponse(it)
        }
        return fresh(key)
    }

    override suspend fun fresh(key: Key): StoreResponse<Output> {
        // nothing is cached, get fetcher
        val fetcherValue = fetcher.get(key)
        fetcherValue.dataOrNull()?.let {
            writer(key, it)
        }
        fetcherValue.errorOrNull()?.let {
            return StoreResponse.ErrorResponse(
                error = it,
                data = null
            )
        }
        return reader(key).singleOrNull()?.let {
            StoreResponse.SuccessResponse(it)
        } ?: StoreResponse.ErrorResponse<Output>(
            error = RuntimeException("reader didn't return any data"),
            data = null
        )
    }

    @Suppress("UNCHECKED_CAST")
    override fun stream(key: Key): Flow<StoreResponse<Output>> {
        return reader(key)
            // TODO: should we really call fetcher.streamFresh ? maybe let developer specify
            // via StoreCall ?
            // the assumption is that we always want to update from backend but what if we
            // don't. Should we instead call just stream? But if it is cached, we are basically
            // re-writing dummy data back because we don't know :/
            .sideCollect(fetcher.streamFresh(key)) { response: StoreResponse<Input> ->
                response.dataOrNull()?.let { data: Input ->
                    writer(key, data)
                }
            }.castNonNull()
    }

    @Suppress("UNCHECKED_CAST")
    override fun streamFresh(key: Key): Flow<StoreResponse<Output>> {
        return fetcher.streamFresh(key)
            .switchMap {
                it.dataOrNull()?.let {
                    writer(key, it)
                }
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

@FlowPreview
private fun <T, R> Flow<T>.sideCollect(
    other: Flow<R>,
    otherCollect: suspend (R) -> Unit
) = flow<StoreResponse<T>> {
    var error: Throwable? = null
    var lastEmitted: T? = null
    var fetched = false
    suspend fun update() {
        val theError = error
        val theEmitted = lastEmitted
        if (theError != null) {
            emit(
                StoreResponse.ErrorResponse(
                    error = theError,
                    data = lastEmitted
                )
            )
        } else if (theEmitted != null) {
            if (fetched) {
                emit(
                    StoreResponse.SuccessResponse(
                        data = theEmitted
                    )
                )
            } else {
                emit(
                    StoreResponse.LoadingResponse(
                        data = theEmitted
                    )
                )
            }
        }
    }
    coroutineScope {
        launch {
            try {
                other.collect {
                    fetched = true
                    otherCollect(it)
                }
            } catch (t: Throwable) {
                error = t
                update()
            }
        }
        this@sideCollect.collect {
            lastEmitted = it
            update()
        }
    }
}