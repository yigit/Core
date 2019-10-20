package com.nytimes.android.external.store3.flow
//
///**
// * https://gist.github.com/streetsofboston/39c3f8c882c9891b35e0ebc3cd812381
// */
//
//
//import kotlinx.coroutines.CancellableContinuation
//import kotlinx.coroutines.CoroutineScope
//import kotlinx.coroutines.InternalCoroutinesApi
//import kotlinx.coroutines.Job
//import kotlinx.coroutines.async
//import kotlinx.coroutines.cancelAndJoin
//import kotlinx.coroutines.coroutineScope
//import kotlinx.coroutines.flow.Flow
//import kotlinx.coroutines.flow.FlowCollector
//import kotlinx.coroutines.flow.collect
//import kotlinx.coroutines.joinAll
//import kotlinx.coroutines.launch
//import kotlinx.coroutines.suspendCancellableCoroutine
//import kotlinx.coroutines.sync.Mutex
//import kotlinx.coroutines.sync.withLock
//import java.util.concurrent.atomic.AtomicInteger
//import java.util.concurrent.atomic.AtomicReference
//import kotlin.coroutines.resume
//import kotlin.coroutines.resumeWithException
//
//class GithubPubish<T>(
//    private val scope: CoroutineScope,
//    private val src: Flow<T>
//) : Publish<T> {
//    val shared = src.share(scope)
//    override fun create(): Flow<T> {
//        return shared
//    }
//
//}
//
///**
// * A [Flow] of type [T] that only starts emitting value after its [connect] method is called.
// *
// * If this flow's [Connection] is still connected, the current [Connection] will be returned when
// * [connect] is called and the flow will not be restarted.
// *
// * When this flow's [collect] method is called, this flow will *not* immediately start collecting. Only after
// * [connect] is called, the emission and actual collecting of values starts.
// */
//interface ConnectableFlow<out T> : Flow<T> {
//    /**
//     * Connects this shared [Flow] to (re-)start collecting values.
//     *
//     * @param scope The [CoroutineScope] in which the shared emissions will take place.
//     * @return The [Connection] that can be closed to stop this shared [Flow].
//     */
//    fun connect(scope: CoroutineScope): Connection
//}
//
///**
// * A connection returned by a call to [ConnectableFlow.connect].
// */
//interface Connection {
//    /**
//     * Returns true if this connection is connected and active.
//     */
//    suspend fun isConnected(): Boolean
//
//    /**
//     * Closes this connection in an orderly fashion.
//     */
//    suspend fun close()
//}
//
///**
// * Publishes and shares an upstream [Flow] of type [T] and returns a [ConnectableFlow] of type [T].
// *
// * The upstream [Flow] begins emissions only after the [ConnectableFlow.connect] has been called.
// *
// * @return A [ConnectableFlow] that represents the shared [Flow] of this receiver.
// */
//fun <T> Flow<T>.publish(): ConnectableFlow<T> = PublishConnectableFlow(this)
//
//fun <T> Flow<T>.replay(bufferSize: Int = 1): ConnectableFlow<T> = if (bufferSize == 1)
//    SingleReplayConnectableFlow(this)
//else
//    ReplayConnectableFlow(this, bufferSize)
//
///**
// * Creates a [Flow] of type [T] from this [ConnectableFlow] that automatically connects (i.e. calls
// * [ConnectableFlow.connect]) when the first [numberOfCollectors] observer starts collecting (i.e. calls [Flow.collect])
// * and automatically cancels this [ConnectableFlow] when the last observers stops collecting.
// *
// * @param scope The scope in which this [ConnectableFlow] will be connected.
// * @param numberOfCollectors The number of observers that need to start collecting before the connection (re)starts.
// * @return A shared referenced-counted [Flow].
// */
//fun <T> ConnectableFlow<T>.refCount(scope: CoroutineScope, numberOfCollectors: Int = 1): Flow<T> =
//    RefCountFlow(this, scope, numberOfCollectors)
//
///**
// * Creates a [Flow] of type [T] from this [ConnectableFlow] that automatically connects (i.e. calls
// * [ConnectableFlow.connect] when at least [numberOfCollectors] observers start collecting.
// *
// * This [ConnectableFlow] will never be automatically canceled and this [ConnectableFlow] will never
// * be restarted. Use [ConnectableFlow.refCount] if you need to have automatic cancellation and restart.
// *
// * @param scope The scope in which this [ConnectableFlow] will be connected.
// * @param numberOfCollectors The number of observers that need to start collecting before the connection starts.
// * @return A shared connection-counted [Flow].
// */
//fun <T> ConnectableFlow<T>.autoConnect(
//    scope: CoroutineScope,
//    numberOfCollectors: Int = 1
//): Flow<T> =
//    AutoConnectFlow(this, scope, numberOfCollectors)
//
///**
// * Shares this [Flow] of type [T] with multiple observers but won't restart when each observer starts
// * collecting. This is the same as calling [Flow.publish] and then [ConnectableFlow.refCount].
// *
// * @param scope The scope in which this [ConnectableFlow] will be connected.
// * @return A new [Flow] that shares this [Flow]
// */
//fun <T> Flow<T>.share(scope: CoroutineScope): Flow<T> = publish().refCount(scope)
//
///**
// * Caches the last [cacheSize] elements of this [Flow] of type [T].
// *
// * When a new observer starts collecting while this cached [Flow] is emitting, it will immediately receive the
// * last [cacheSize] elements emitted by the [Flow]. This is the same as calling [Flow.replay] and
// * then [ConnectableFlow.autoConnect].
// *
// * @param scope The scope in which this [ConnectableFlow] will be connected.
// * @param cacheSize The size of the cache
// * @return A new [Flow] that caches the last [cacheSize] emission of this [Flow]
// */
//fun <T> Flow<T>.cache(scope: CoroutineScope, cacheSize: Int = 1): Flow<T> =
//    replay(cacheSize).autoConnect(scope)
//
//private abstract class ConnectableFlowImpl<T>(private val upStream: Flow<T>) : ConnectableFlow<T> {
//    private val collectors: MutableList<CollectorInfo<T>> = mutableListOf()
//
//    private val connection: ConnectionImpl = ConnectionImpl()
//
//    @InternalCoroutinesApi
//    override suspend fun collect(collector: FlowCollector<in T>) = coroutineScope {
//        onCollect(this, collector)
//
//        suspendCancellableCoroutine<Unit> { cont ->
//            val observerInfo = CollectorInfo(collector, this, cont)
//
//            cont.invokeOnCancellation {
//                collectors.runSync {
//                    this -= observerInfo
//                }
//            }
//
//            collectors.runSync {
//                this += observerInfo
//            }
//        }
//    }
//
//    override fun connect(scope: CoroutineScope): Connection {
//        return connection.apply {
//            scope.fetchJob { startConnection(this) }
//        }
//    }
//
//    protected open fun onCollect(collectionScope: CoroutineScope, collector: FlowCollector<in T>) {}
//
//    protected open fun onEmit(value: T) {}
//
//    protected open fun onCloseConnection(connectionScope: CoroutineScope) {}
//
//    private fun startConnection(connectionScope: CoroutineScope): Job {
//        return connectionScope.launch {
//            try {
//                upStream.collect(::notifyCollectors)
//                completeCollection()
//            } catch (e: Throwable) {
//                failCollection(e)
//            } finally {
//                collectors.runSync { clear() }
//                onCloseConnection(connectionScope)
//            }
//        }
//    }
//
//    private suspend fun notifyCollectors(value: T) {
//        onEmit(value)
//
//        val jobs = collectors.copySync().map { (collector, collectionScope, _) ->
//            // Switch to the scope of the downstream collector.
//            collectionScope.launch {
//                collector.emit(value)
//            }
//        }
//        jobs.joinAll()
//    }
//
//    private fun completeCollection() {
//        collectors.copySync().forEach { (_, _, cont) ->
//            cont.resume(Unit)
//        }
//    }
//
//    private fun failCollection(e: Throwable) {
//        collectors.copySync().forEach { (_, _, cont) ->
//            cont.resumeWithException(e)
//        }
//    }
//}
//
//private class PublishConnectableFlow<T>(upStream: Flow<T>) : ConnectableFlowImpl<T>(upStream)
//
//private class SingleReplayConnectableFlow<T>(upStream: Flow<T>) : ConnectableFlowImpl<T>(upStream) {
//    private var buffer = AtomicReference<Optional<T>>(None)
//
//    override fun onCollect(collectionScope: CoroutineScope, collector: FlowCollector<in T>) {
//        collectionScope.launch {
//            when (val optional = buffer.get()) {
//                is Some -> collector.emit(optional.value)
//            }
//        }
//    }
//
//    override fun onEmit(value: T) {
//        buffer.set(Some(value))
//    }
//
//    override fun onCloseConnection(connectionScope: CoroutineScope) {
//        buffer.set(None)
//    }
//}
//
//private class ReplayConnectableFlow<T>(
//    upStream: Flow<T>,
//    private val bufferSize: Int
//) : ConnectableFlowImpl<T>(upStream) {
//
//    private val buffer = CircularArray<T>(bufferSize)
//
//    override fun onCloseConnection(connectionScope: CoroutineScope) {
//        clearBuffer()
//    }
//
//    override fun onCollect(collectionScope: CoroutineScope, collector: FlowCollector<in T>) {
//        collectionScope.launch {
//            copyOfBuffer().forEach { collector.emit(it) }
//        }
//    }
//
//    override fun onEmit(value: T) {
//        addToBuffer(value)
//    }
//
//    private fun clearBuffer() = buffer.runSync { clear() }
//
//    private fun copyOfBuffer() = buffer.runSync { Array<Any?>(size(), ::get) } as Array<T>
//
//    private fun addToBuffer(value: T) = buffer.runSync {
//        if (size() == bufferSize) {
//            popFirst()
//        }
//        addLast(value)
//    }
//}
//
//private class RefCountFlow<T>(
//    private val upStream: ConnectableFlow<T>,
//    private val scope: CoroutineScope,
//    private val numberOfCollectors: Int
//) : Flow<T> {
//    private var refCount = AtomicInteger(0)
//
//    private lateinit var connection: Connection
//
//    private val connectionMutex = Mutex()
//
//    @InternalCoroutinesApi
//    override suspend fun collect(collector: FlowCollector<in T>) {
//        scope.launch {
//            connectionMutex.withLock {
//                if (refCount.incrementAndGet() == numberOfCollectors) {
//                    connection = upStream.connect(scope)
//                }
//            }
//        }.join()
//
//        try {
//            upStream.collect(collector)
//        } finally {
//            scope.launch {
//                connectionMutex.withLock {
//                    if (refCount.decrementAndGet() == 0) {
//                        connection.close()
//                    }
//                }
//            }.join()
//        }
//    }
//}
//
//private class AutoConnectFlow<T>(
//    private val upStream: ConnectableFlow<T>,
//    private val scope: CoroutineScope,
//    private val numberOfCollectors: Int
//) : Flow<T> {
//    private var refCount = AtomicInteger(0)
//
//    private var connection: Connection? = null
//
//    private val connectionMutex = Mutex()
//
//    init {
//        if (numberOfCollectors <= 0) {
//            scope.launch {
//                connectionMutex.withLock {
//                    connection = upStream.connect(this@launch)
//                }
//            }
//        }
//    }
//
//    @InternalCoroutinesApi
//    override suspend fun collect(collector: FlowCollector<in T>) = coroutineScope {
//        val upStreamHasTerminated = scope.async {
//            connectionMutex.withLock {
//                val connection = if (refCount.incrementAndGet() == numberOfCollectors) {
//                    upStream.connect(scope)
//                } else {
//                    connection
//                }
//                this@AutoConnectFlow.connection = connection
//                connection != null && !connection.isConnected()
//            }
//        }.await()
//
//        if (!upStreamHasTerminated) {
//            upStream.collect(collector)
//        }
//    }
//}
//
//private class ConnectionImpl : Connection {
//    private var job: Job? = null
//
//    private val isJobActive get() = job?.isActive == true
//
//    private val jobMutex = Mutex()
//
//    override suspend fun isConnected(): Boolean {
//        return jobMutex.withLock { isJobActive }
//    }
//
//    override suspend fun close() {
//        jobMutex.withLock {
//            job?.cancelAndJoin()
//            job = null
//        }
//    }
//
//    fun CoroutineScope.fetchJob(getJob: suspend CoroutineScope.() -> Job) {
//        launch {
//            jobMutex.withLock {
//                if (!isJobActive) {
//                    job = getJob()
//                }
//            }
//        }
//    }
//}
//
//private data class CollectorInfo<T>(
//    val collector: FlowCollector<in T>,
//    val scope: CoroutineScope,
//    val cont: CancellableContinuation<Unit>
//)
//
//private inline fun <A : Any, B> A.runSync(block: A.() -> B): B = synchronized(this) { block() }
//
//private inline fun <A> MutableList<A>.copySync() = runSync { toList() }
//
//private sealed class Optional<out T>
//private class Some<out T>(val value: T) : Optional<T>()
//private object None : Optional<Nothing>()