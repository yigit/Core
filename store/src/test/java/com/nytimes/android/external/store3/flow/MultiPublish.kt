package com.nytimes.android.external.store3.flow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class EndlessPublishFlow<T>(
    private val scope: CoroutineScope,
    private val creator: () -> Flow<T>
) : Publish<T> {
    private val flowMutex = Mutex()
    private val stateMutex = Mutex()
    private var activeJob = AtomicReference<Job>(IDLE_JOB)
    private val activeChannels = CopyOnWriteArrayList<Channel<Emission<T>>>()
    private val generation = AtomicInteger(0)
    private val deliveredEmissionVersions = Channel<Emission<T>>(Channel.UNLIMITED)
    private var currentState: State = State.IDLE
    val state
        get() = currentState

    private suspend fun setState(newState: State) {
        println("new state:$newState")

        stateMutex.withLock {
            if (newState == State.RUNNING) {
                check(state == State.IDLE) {
                    state
                }
                currentState = State.RUNNING
                startConsumer()
            } else if (newState == State.IDLE) {
                currentState = State.IDLE
                finishConsumer()
            }
        }
    }

    private fun startConsumer() {
        val newJob = scope.launch(start = CoroutineStart.LAZY) {
            val myGeneration = generation.incrementAndGet()
            creator().collectIndexed { version, value ->
                val emission = Emission(
                    generation = myGeneration,
                    version = version,
                    data = value
                )
                // this yield is a test workaround because TestDispatcher eagerly evaluates
                // registered jobs w/o considering first come first serve
                yield()
                println("source:$emission ${activeChannels.size}")
                // send data to each channel, they are unlimited so this should be instant
                activeChannels.forEach { receiver ->
                    receiver.send(emission)
                }
                // wait for an ack so that we know we've delivered it to at least 1
                while (true) {
                    val received = deliveredEmissionVersions.receive()
                    if (received.generation == myGeneration && received.version == version) {
                        println("delivered $received")
                        break
                    }
                }
            }
            setState(State.IDLE)
        }
        if (activeJob.compareAndSet(IDLE_JOB, newJob)) {
            newJob.start()
        }
    }

    private fun clearState(): Boolean {
        val current = activeJob.get()
        return activeJob.compareAndSet(current, IDLE_JOB)
    }

    private fun finishConsumer() {
        if (clearState()) {
            activeChannels.forEach {
                it.close()
            }
        }
    }

    override fun create(): Flow<T> {
        return flow<T> {
            val currentChannel = Channel<Emission<T>>(Channel.UNLIMITED)
            try {
                inc(currentChannel)
                emitAll(currentChannel.consumeAsFlow().onEach {
                    deliveredEmissionVersions.send(it)
                }.map {
                    it.data
                })
            } finally {
                dec(currentChannel)
            }
        }
    }

    private suspend fun inc(channel: Channel<Emission<T>>) {
        flowMutex.withLock {
            activeChannels.add(channel)
            if (activeChannels.size == 1) {
                setState(State.RUNNING)
            }
        }
    }

    private suspend fun dec(channel: Channel<Emission<T>>) {
        flowMutex.withLock {
            activeChannels.remove(channel)
            if (activeChannels.size == 0) {
                setState(State.IDLE)
            }
        }
    }

    enum class State {
        IDLE,
        RUNNING
    }

    companion object {
        private val IDLE_JOB = Job()
    }

    private data class Emission<T>(
        val generation: Int,
        val version: Int,
        val data: T
    )
}