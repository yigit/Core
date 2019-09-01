package com.nytimes.android.external.store3.pipeline

/**
 * Holder for responses from Store.
 *
 * Instead of using regular error channels (a.k.a. throwing exceptions), Store uses this holder
 * class to represent each response. This allows the flow to keep running even if an error happens
 * so that if there is an observable single source of truth, application can keep observing it.
 */
sealed class StoreResponse<T> {
    data class Loading<T>(val data: T? = null) : StoreResponse<T>()
    data class Error<T>(val error: Throwable, val data: T? = null) : StoreResponse<T>()
    data class Success<T>(val data: T) : StoreResponse<T>()

    /**
     * Returns the available data or throws [NullPointerException] if there is no data.
     */
    fun requireData(): T {
        return when (this) {
            is Loading -> data ?: throw NullPointerException("there is no data")
            is Error -> data ?: throw error
            is Success -> data
        }
    }

    /**
     * If this [StoreResponse] is of type [StoreResponse.Error], throws the exception
     * Otherwise, does nothing.
     */
    fun throwIfError() {
        if (this is Error) {
            throw error
        }
    }

    /**
     * If this [StoreResponse] is of type [StoreResponse.Error], returns the available error
     * from it. Otherwise, returns `null`.
     */
    fun errorOrNull(): Throwable? {
        return (this as? Error)?.error
    }

    /**
     * If there is data available (either [StoreResponse.Loading] with data or
     * [StoreResponse.Success], returns it; otherwise returns null.
     */
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