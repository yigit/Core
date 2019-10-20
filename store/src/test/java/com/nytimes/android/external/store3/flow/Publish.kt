package com.nytimes.android.external.store3.flow

import kotlinx.coroutines.flow.Flow

interface Publish<T> {
    fun create() : Flow<T>
}