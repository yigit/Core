package com.nytimes.android.external.store3.flow2

import com.nytimes.android.external.store3.flow.Publish
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow

// TODO
//  if a later created one finishes w/o any value, we should re-start the flow for that one.
//  this will allow us to nicely handle late arrivals.
class ActorPublish<T>(
    private val scope : CoroutineScope,
    private val source: () -> Flow<T>
) : Publish<T> {
    private var current : SharedFlow<T>? = null
    override fun create(): Flow<T> {
        // TODO make thread safe
        if (current?.isActive == true) {
            return current!!.create()
        } else {
            return SharedFlow(scope, source()).also {
                current = it
            }.create()
        }
    }
}