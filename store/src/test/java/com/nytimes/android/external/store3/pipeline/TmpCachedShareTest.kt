package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.broadcast
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.ArrayDeque
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private val CACHE_LIMIT = 2

private class IdleChannelCounter {
    private var _version = 0
    private val mutex = Mutex()
    val newVersionPing = Channel<Unit>()
    val version
        get() = _version

    suspend fun onIdle(version: Int) {
        if (version <= _version) return
        mutex.withLock {
            if (version <= _version) return
            _version = version
        }
        newVersionPing.send(Unit)
    }
}

private class SubFlowManager<T> {
    private val subFlows = CopyOnWriteArrayList<SubFlow<T>>()
    private val lock = ReentrantLock() // cannot use Mutex, create is not suspend
    val activeChannelCnt = Channel<Int>(Channel.CONFLATED)

    fun create(
        counter: IdleChannelCounter,
        initialItems: Collection<Pair<Int, T>>
    ) = SubFlow(
        counter = counter,
        initialValues = initialItems
    ).also {
        val newSize = lock.withLock {
            subFlows.add(it)
            subFlows.size
        }
        activeChannelCnt.offer(newSize)
    }

    suspend fun send(value: Pair<Int, T>) {
        subFlows.forEach {
            it.send(value)
        }
    }

    suspend fun close() {
        subFlows.forEach {
            it.close()
        }
    }
}

private class SubFlow<T>(
    private val counter: IdleChannelCounter,
    initialValues: Collection<Pair<Int, T>>
) {
    val pendingValues = Channel<Pair<Int, T>>(CACHE_LIMIT).also { pending ->
        initialValues.forEach { value ->
            pending.offer(value)
        }
    }

    fun asFlow() = flow<T> {
        log("init sub flow ${this@SubFlow}")
        pendingValues.consumeEach {
            emit(it.second)
            counter.onIdle(it.first)
        }
        log("pending values done ${this@SubFlow}")
    }

    suspend fun send(pair: Pair<Int, T>) {
        pendingValues.send(pair)
    }

    suspend fun close() {
        pendingValues.close()
    }
}

class CacheSharer<T> (
    private val flow: Flow<T>,
    private val scope: CoroutineScope
) {
    private var lock = ReentrantLock()
    private var sharedFlow: SharedFlow<T>? = null
    fun sharedFlow(): Flow<T> {
        TODO()
    }
}

class SharedFlow<T>(
    private val flow: Flow<T>,
    private val scope: CoroutineScope,
    private val closeCallback : suspend () -> Unit
) {
    private val closed = AtomicBoolean(false)
    private val broadcastChannel = scope.broadcast(
        capacity = 1
    ) {
        log("broadcasting begin")
        flow.collect {
            log("start collecting next element from original")
            send(it)
            log("sent $it from original")
        }
        log("broadcasting done")
    }
    private val idleCounter = IdleChannelCounter()
    private val subFlowManager = SubFlowManager<T>()

    private enum class State {
        WAITING,
        ACTIVE
    }

    private var state: State = State.WAITING
    private val valueCache = ArrayDeque<Pair<Int, T>>()

    @InternalCoroutinesApi
    val distributor = scope.launch {
        var subscription: ReceiveChannel<T>? = null
        try {
            while (true) {
                when (state) {
                    State.WAITING -> {
                        log("status is waiting")
                        if (subFlowManager.activeChannelCnt.receive() >= 1) {
                            log("going active")
                            state = State.ACTIVE
                            subscription = broadcastChannel.openSubscription()
                            log("opened subscription")
                        }
                    }
                    State.ACTIVE -> {
                        log("status is active")
                        select<Unit> {
                            subFlowManager.activeChannelCnt.onReceive { value ->
                                log("received active cnt $value")
                                if (value < 1) {
                                    log("going to waiting")
                                    state = State.WAITING
                                    subscription?.cancel()
                                }
                            }
                            subscription!!.onReceive { value ->
                                log("common received value $value")
                                val version = idleCounter.version
                                val toSend = (version + 1) to value
                                valueCache.offerLast(toSend)
                                log("dispatching $toSend")
                                // clear extra cached elements
                                while (valueCache.size > CACHE_LIMIT) valueCache.pollFirst()
                                subFlowManager.send(toSend)
                                // wait for an idle flow before continiuing
                                idleCounter.newVersionPing.receive()
                            }
                        }
                    }
                }
            }
        } catch (closed: ClosedReceiveChannelException) {
            log("channel closed ")
        }
        subFlowManager.close()
        closed.set(true)
    }

    fun sharedFlow(): Flow<T>? {
        if (closed.get()) {
            return null
        }
        return subFlowManager.create(
            counter = idleCounter,
            initialItems = valueCache // TODO sync!!
        ).asFlow()
    }
}

@RunWith(JUnit4::class)
class TmpCachedShareTest {
    val scope = TestCoroutineScope()
    //val scope = CoroutineScope(SupervisorJob())
    @Test
    fun sharing() = scope.runBlockingTest {
        if (true) {
            return@runBlockingTest
        }
        val producer = flow<Int> {
            (0..5).forEach {
                log("will emit $it")
                emit(it)
                log("sent $it")
                delay(100)
            }
        }
        val cached = CacheSharer(producer, this)
        log("will start first flow")
        cached.sharedFlow().collect {
            println("received[1] $it")
        }
        log("first flow done, will start second")
        cached.sharedFlow().collect {
            println("received[2] $it")
        }
        log("second flow done")
        delay(1_000)
        log("DONE")
    }
}

private fun log(msg: Any?) {
    println("[${Thread.currentThread().name}]: $msg")
}