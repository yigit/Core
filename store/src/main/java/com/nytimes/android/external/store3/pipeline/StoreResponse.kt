package com.nytimes.android.external.store3.pipeline

sealed class StoreResponse<T> {
    data class Loading<T>(val data: T? = null) : StoreResponse<T>()
    data class Error<T>(val error: Throwable, val data: T? = null) : StoreResponse<T>()
    data class Success<T>(val data: T) : StoreResponse<T>()

    fun requireData(): T {
        return when (this) {
            is Loading -> data ?: throw NullPointerException("there is no data")
            is Error -> data ?: throw error
            is Success -> data
        }
    }

    fun throwIfError() {
        if (this is Error) {
            throw error
        }
    }

    fun errorOrNull(): Throwable? {
        return (this as? Error)?.error
    }

    fun dataOrNull(): T? = when (this) {
        is Loading -> data
        is Error -> data
        is Success -> data
    }

    internal fun <V> swapData(data: V?): StoreResponse<V> {
        // returns same type
        return when (this) {
            is Loading -> Loading(data)
            is Error -> Error(error, data)
            is Success -> data?.let {
                Success(data)
            } ?: throw IllegalArgumentException("data cannot be null for success response")
        }
    }
}