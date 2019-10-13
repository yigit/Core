package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.broadcastIn
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Class that handles combining multiple store requests to avoid sending too many requests down
 * to the Pipeline.
 *
 * This one is created per `Key` and is able to create multiplexed flows.
 */
@FlowPreview
class StoreRequestKeyManager<Key, Output>(
    private val pipeline: PipelineStore<Key, Output>,
    private val key: Key,
    private val scope: CoroutineScope
) {
    private val maxDistributedVersionChannel = Channel<Int>(
        Channel.RENDEZVOUS
    )
    private var maxVersion = 0

    private val clerkCallback = object : ClerkCallback<Output> {
        override suspend fun requestVersion(version: Int) {
            maxDistributedVersionChannel.send(version)
        }

        private val readingFlow = channelFlow {
            pipeline
                .stream(StoreRequest.cached(key = key, refresh = true))
                .collect {
                    send(it)
                    // wait until next version is requested
                    var version = maxVersion
                    while (version <= maxVersion) {
                        version = maxDistributedVersionChannel.receive()
                    }
                    maxVersion = version
                }

        }

        override val mainChannel = readingFlow.broadcastIn(scope)

        override suspend fun onComplete(clerk: Clerk<Output>) {
            TODO("remove from list")
        }

    }
    private var latestVersion = 0

    private val clerks = mutableListOf<Clerk<Output>>()
    private val clerkLock = ReentrantLock() // cannot use mutex, stream is not a suspend function
    fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        check(request.key == key) {
            "Provided key (${request.key}) does not match manager's key (${key})"
        }
        val clerk = Clerk(maxVersion, clerkCallback)
        clerkLock.withLock {
            clerks.add(clerk)
        }
        return clerk.flow
    }
}

@FlowPreview
private interface ClerkCallback<Output> {
    suspend fun requestVersion(version: Int)
    val mainChannel: BroadcastChannel<StoreResponse<Output>>
    suspend fun onComplete(clerk: Clerk<Output>)
}

// handles 1 client
@FlowPreview
private class Clerk<Output>(
    private var version: Int,
    private val callback: ClerkCallback<Output>
) {
    val flow = channelFlow<StoreResponse<Output>> {
        val subscription = callback.mainChannel.openSubscription()
        try {
            while (!subscription.isClosedForReceive) {
                callback.requestVersion(version++)
                send(subscription.receive())
            }
        } catch (closed: ClosedReceiveChannelException) {
            // is fine
        } finally {
            subscription.cancel()
        }
    }
//    suspend fun send(item: StoreResponse<Output>, version: Int) {
//        channel.send(item)
//        callback.onSent(this, version)
//    }
}