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
    override suspend fun get(request: StoreRequest<Key>): StoreResponse<Output> {
        val value: Output? = if (request.shouldSkipCache(CacheType.DISK)) {
            null
        } else {
            reader(request.key).singleOrNull()
        }
        value?.let {
            // cached value from persister
            return StoreResponse.SuccessResponse(it)
        }
        // skipped cache or cache is null
        val fetcherValue = fetcher.get(request)
        fetcherValue.dataOrNull()?.let {
            writer(request.key, it)
        }
        fetcherValue.errorOrNull()?.let {
            return StoreResponse.ErrorResponse(
                error = it,
                data = null
            )
        }
        return reader(request.key).singleOrNull()?.let {
            StoreResponse.SuccessResponse(it)
        } ?: StoreResponse.ErrorResponse<Output>(
            error = RuntimeException("reader didn't return any data"),
            data = null
        )
    }

    @Suppress("UNCHECKED_CAST")
    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        if (request.shouldSkipCache(CacheType.DISK)) {
            return fetcher.stream(request)
                .switchMap {
                    it.dataOrNull()?.let {
                        writer(request.key, it)
                    }
                    reader(request.key)
                }.castNonNull()
        } else {
            return reader(request.key)
                // TODO: we need to get more information from the request to decide whether we
                //  should refresh from network or not
                .sideCollect(fetcher.stream(request)) { response: StoreResponse<Input> ->
                    response.dataOrNull()?.let { data: Input ->
                        writer(request.key, data)
                    }
                }.castNonNull()
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