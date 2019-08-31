package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield

class PipelinePersister<Key, Input, Output>(
    private val fetcher: PipelineStore<Key, Input>,
    private val reader: (Key) -> Flow<Output?>,
    private val writer: suspend (Key, Input) -> Unit,
    private val delete: (suspend (Key) -> Unit)? = null
) : PipelineStore<Key, Output> {
    @ExperimentalCoroutinesApi
    @Suppress("UNCHECKED_CAST")
    override fun stream(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return when {
            request.shouldSkipCache(CacheType.DISK) -> fetchSkippingCache(request)
            request.refresh -> diskNetworkCombined(
                request = request,
                alwaysTriggerNetwork = true
            )
            else -> diskNetworkCombined(
                request = request,
                alwaysTriggerNetwork = false
            )
        }
    }

    /**
     * skipping cache, just delegate to the fetcher but update disk w/ any new data from fetcher
     */
    private fun fetchSkippingCache(request: StoreRequest<Key>): Flow<StoreResponse<Output>> {
        return fetcher.stream(request)
            .flatMapLatest { response: StoreResponse<Input> ->
                // explicit type is necessary for type resolution
                flow<StoreResponse<Output>> {
                    if (response is StoreResponse.Loading) {
                        // send as is, nothing to save into database
                        emit(StoreResponse.Loading())
                    } else {
                        // save into database first
                        writer(request.key, response.requireData())
                        // continue database data
                        val readerFlow: Flow<StoreResponse<Output>> =
                            reader(request.key).mapNotNull {
                                it?.let {
                                    // keep the response type to carry over error, if there is
                                    response.swapData(it)
                                }
                            }
                        emitAll(readerFlow)
                    }
                }
            }
    }

    /**
     * we want to stream from disk but also want to refresh. Immediately start fetcher when flow
     * starts.
     *
     * If [alwaysTriggerNetwork] is set to [false], we want to return from disk but also fetch from
     * the server if there is nothing in disk.
     * To do that, we need to see the first disk value and then decide to fetch or not.
     * in any case, we always return the Flow from reader.
     */
    private fun diskNetworkCombined(
        request: StoreRequest<Key>,
        alwaysTriggerNetwork: Boolean
    ) = channelFlow<StoreResponse<Output>> {
        val diskCommands = Channel<DiskCommand>(capacity = Channel.RENDEZVOUS)
        val networkChannel = Channel<StoreResponse<Input>?>(capacity = Channel.RENDEZVOUS)
        launch {
            diskCommands.send(DiskCommand.READ_FIRST)
            networkChannel.send(null)
        }
        val networkLock = CompletableDeferred<Boolean>()
        val networkJob = launch {
            if (networkLock.await()) {
                // TODO get rid of this yield
                //  right now, it is important for tests to avoid losing a loading command
                //  since everything happend immediately, it may not have chance to dispatch
                //  normally, it is unlikely to happen in an app and also it is OK to send
                //  Success without loading. That being said, we could try to chance the bottom
                //  operation to always consume all events to avoid this.
                yield()
                fetcher.stream(request).collect {
                    val networkData = it.dataOrNull()
                    if (networkData != null) {
                        try {
                            val stopAck = CompletableDeferred<Unit>()
                            diskCommands.send(DiskCommand.STOP(stopAck))
                            stopAck.await() // make sure disk stops before writing
                            // WRITE
                            writer(request.key, networkData)
                        } finally {
                            // restart reading for the new data w/ the new state
                            // TODO use a separate class to avoid holding into data
                            diskCommands.send(DiskCommand.READ(it))
                        }
                    } else {
                        networkChannel.send(it)
                    }
                }
            }
        }

        val controlledDiskFlow = diskCommands.consumeAsFlow().flatMapLatest { command ->
            when (command) {
                is DiskCommand.READ_FIRST -> {
                    reader(request.key)
                        .onEach { diskData ->
                            if (diskData == null || alwaysTriggerNetwork) {
                                if (networkLock.isActive) {
                                    networkLock.complete(true)
                                }
                            } else {
                                // don't go to network
                                networkLock.complete(false)
                            }
                        }
                        .map {
                            command to it
                        }
                }
                is DiskCommand.READ<*> -> {
                    reader(request.key).map {
                        command to it
                    }
                }
                is DiskCommand.STOP -> {
                    command.ack.complete(Unit)
                    emptyFlow()
                }
            }
        }
        val finalResult: Flow<StoreResponse<Output>?> =
            controlledDiskFlow.combine(networkChannel.consumeAsFlow()) { (command, disk), network ->
                when (command) {
                    DiskCommand.READ_FIRST -> {
                        if (network == null) {
                            // no network, decide on nullness and eagerness
                            val disked: StoreResponse<Output>? = if (disk == null) {
                                // no disk, no network; don't send anything
                                // TODO what if network does not send Loading, we should handle
                                //  that and maybe send loading here then try to avoid for dupes?
                                //  right now, there is no API to provide it but when it comes
                                //  (if it comes) we need to support it. Might use combine with
                                //  attribution to avoid duplicate events?
                                null
                            } else {
                                // there is disk data, decide whether we'll call network and assume
                                if (alwaysTriggerNetwork) {
                                    // wait for network first. it will send loading then we'll
                                    // send the data
                                    null
                                    //StoreResponse.Loading<Output>(disk)
                                } else {
                                    StoreResponse.Success<Output>(disk)
                                }
                            }
                            disked
                        } else {
                            network.swapData(disk)
                        }
                    }
                    is DiskCommand.READ<*> -> {
                        // TODO what if network writes and then it gets deleted?
                        //  maybe we need to accept null or do something else ?
                        (command as DiskCommand.READ<Input>).networkState.swapData(disk)
                    }
                    else -> {
                        throw IllegalStateException("unexpected disk command $command")
                    }
                }
            }
        finalResult.collect {
            if (it != null) {
                send(it)
            }
        }
    }

    override suspend fun clear(key: Key) {
        fetcher.clear(key)
        delete?.invoke(key)
    }

    sealed class DiskCommand {
        object READ_FIRST : DiskCommand()
        class READ<T>(val networkState: StoreResponse<T>) : DiskCommand()
        class STOP(val ack: CompletableDeferred<Unit>) : DiskCommand()
    }
}
