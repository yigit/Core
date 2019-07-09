package com.nytimes.android.external.store3.pipeline

sealed class StoreResponse<Output> {
    class LoadingResponse<Output>(val data: Output?) : StoreResponse<Output>()
    class SuccessResponse<Output>(val data: Output) : StoreResponse<Output>()
    class ErrorResponse<Output>(val error: Throwable, val data: Output?) : StoreResponse<Output>()

    fun isError() = this is ErrorResponse
    fun isLoading() = this is LoadingResponse
    fun isSuccess() = this is SuccessResponse

    fun dataOrThrow(): Output? {
        return when (this) {
            is LoadingResponse -> data
            is SuccessResponse -> data
            is ErrorResponse -> throw error
        }
    }

    fun dataOrNull(): Output? {
        return when (this) {
            is LoadingResponse -> data
            is SuccessResponse -> data
            is ErrorResponse -> null
        }
    }

    fun errorOrNull() : Throwable? {
        return (this as? ErrorResponse)?.error
    }

    fun throwIfError(): StoreResponse<Output> {
        if (this is ErrorResponse) {
            throw error
        }
        return this
    }

    internal suspend fun <NewOutput> mapData(converter: suspend (Output) -> NewOutput): StoreResponse<NewOutput> {
        return when (this) {
            is LoadingResponse -> LoadingResponse(data?.let { converter(data) })
            is SuccessResponse -> SuccessResponse(converter(data))
            is ErrorResponse -> ErrorResponse(error, data?.let { converter(data) })
        }
    }
}