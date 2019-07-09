package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

private object NotReceived

// this is actually in the library, not in our version yet or not released
@FlowPreview
internal suspend fun <T> Flow<T>.singleOrNull(): T? {
    var value: Any? = NotReceived
    try {
        collect {
            value = it
            throw AbortFlowException()
        }
    } catch (abort: AbortFlowException) {
        // expected
    }
    return if (value === NotReceived) {
        null
    } else {
        @Suppress("UNCHECKED_CAST")
        value as? T
    }
}

@FlowPreview
internal fun <T> Flow<T>.mapToStoreResponse(): Flow<StoreResponse<T>> {
    return flow {
        var lastItem: T? = null
        try {
            collect {
                lastItem = it
                emit(StoreResponse.SuccessResponse(it))
            }
        } catch (cancelation: CancellationException) {
            throw cancelation
        } catch (throwable: Throwable) {
            emit(
                StoreResponse.ErrorResponse<T>(
                    data = lastItem,
                    error = throwable
                )
            )
        }
    }
}

// take from Errors.kt
@FlowPreview
internal fun <T> Flow<T>.storeCollectSafely(onException: suspend FlowCollector<T>.(Throwable, lastEmitted: T?) -> Unit): Flow<T> =
    flow {
        // Note that exception may come from the downstream operators, we should not switch on that
        var fromDownstream = false
        var lastEmitted: T? = null
        try {
            collect {
                try {
                    lastEmitted = it
                    emit(it)
                } catch (e: Throwable) {
                    fromDownstream = true
                    throw e
                }
            }
        } catch (e: Throwable) {
            if (fromDownstream) throw e
            onException(e, lastEmitted)
        }
    }