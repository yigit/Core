package com.nytimes.android.external.store3.flow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.receiveOrNull
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class ItemSet<T>(
    // using a callback instead of flow to make test dispatcher happy :/
    private val observerCountChangeCallback : suspend (ItemSet<T>, Boolean, Int) -> Unit
) {
    private val items = mutableListOf<Channel<T>>()
    private val sizeChannel = ConflatedBroadcastChannel<Int>()
    private val mutex = Mutex()
    private var generation : Int = 0

    suspend fun sendAll(data:T) {
        val copy = mutex.withLock {
            ArrayList(items)
        }
        copy.forEach {
            it.send(data)
        }
    }

    suspend fun closeAll() {
        while (true) {
            val toBeStopped = mutex.withLock {
                if (items.isEmpty()) {
                    null
                } else {
                    items.removeAt(items.size - 1)
                }
            }
            if (toBeStopped == null) {
                break
            } else {
                remove(toBeStopped)
            }
        }
    }

    suspend fun add(t: Channel<T>) : Int {
        var nextGen : Int? = null
        return mutex.withLock {
            items.add(t)
            if (items.size == 1) {
                generation ++
                nextGen = generation
            }
            generation
        }.also {
            if (nextGen != null) {
                observerCountChangeCallback(this, true, nextGen!!)
            }
        }
    }

    suspend fun remove(t: Channel<T>) {
        var prevGen :Int? = null
        mutex.withLock {
            items.remove(t)
            if(items.isEmpty()) {
                prevGen = generation
            }
        }
        t.close()
        if (prevGen != null) {
            observerCountChangeCallback(this, false, prevGen!!)
        }
    }
}

class EndlessPublishFlow2<T>(
    private val scope: CoroutineScope,
    private val source: () -> Flow<T>
) : Publish<T> {
    private var currentJob:Job? = null
    private val jobMutex = Mutex()
    private val collectorSet = ItemSet<Emission<T>> {itemSet, active, myGeneration ->
        jobMutex.withLock {
            if (active) {
                check(currentJob == null) {
                    "current job should'be been null :/"
                }
                currentJob = scope.launch {
                    var latestRequestedVersion = -1
                    while(true) {
                        val requested = requestChannel.receive()
                        if (requested.version > latestRequestedVersion) {
                            latestRequestedVersion = requested.version
                            break
                        }
                    }
                    source().collect {
                        itemSet.sendAll(Emission(
                            generation = myGeneration,
                            version = latestRequestedVersion,
                            data = it
                        ))
                        while(true) {
                            val requested = requestChannel.receive()
                            if (requested.version > latestRequestedVersion) {
                                latestRequestedVersion = requested.version
                                break
                            }
                        }
                    }
                    itemSet.closeAll()
                }
            } else {
                val job = checkNotNull(currentJob)
                job.cancel()
            }
        }
    }
    private val requestChannel = Channel<EmissionRequest>(Channel.UNLIMITED)
    override fun create() : Flow<T> {
        return flow<T> {
            val myChannel = Channel<Emission<T>>(Channel.UNLIMITED)
            try {
                val myGeneration = collectorSet.add(myChannel)
                requestChannel.send(
                    EmissionRequest(
                        generation = myGeneration,
                        version = 0
                    )
                )
                myChannel.consumeEach {
                    emit(it.data)
                    requestChannel.send(
                        EmissionRequest(
                            generation = myGeneration,
                            version = it.version + 1
                        )
                    )
                }
                println("done")
            } catch (th:Throwable){
                println("error:")
                th.printStackTrace()
            } finally {
                collectorSet.remove(myChannel)
            }
        }
    }
    private data class EmissionRequest(
        val generation: Int,
        val version:Int
    )

    private data class Emission<T>(
        val generation:Int,
        val version:Int,
        val data:T
    )
}