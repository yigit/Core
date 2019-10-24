package com.nytimes.android.external.store3.flow2

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch


@ExperimentalCoroutinesApi
class SharedFlowProducer<T>(
    private val scope: CoroutineScope,
    private val src: Flow<T>,
    private val channelManager: ChannelManager<T>
) {
    private lateinit var collectionJob: Job

    fun start() {
        scope.launch {
            channelManager.active.await()
            collectionJob = scope.launch {
                src.catch {
                    channelManager.send(Message.DispatchError(it))
                }.collect {
                    val ack = CompletableDeferred<Unit>()
                    channelManager.send(Message.DispatchValue(it, ack))
                    // suspend until at least 1 receives the new value
                    ack.await()
                }
            }
            scope.launch {
                channelManager.finished.await()
                collectionJob.cancel()
            }
            collectionJob.join()
            channelManager.send(Message.Cleanup())
        }
    }
}