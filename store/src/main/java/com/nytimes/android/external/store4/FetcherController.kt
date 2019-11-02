package com.nytimes.android.external.store4

import com.nytimes.android.external.store4.multiplex.Multiplexer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@FlowPreview
@ExperimentalCoroutinesApi
internal class FetcherController<Key, Input, Output>(
    private val scope: CoroutineScope,
    private val realFetcher: (Key) -> Flow<Input>,
    private val sourceOfTruth: SourceOfTruthWithBarrier<Key, Input, Output>
) {
    private val fetchers = mutableMapOf<Key, Multiplexer<Input>>()
    private val multiplexerLock = ReentrantLock()
    fun getFetcher(key: Key): Flow<Input> {
        val multiplexer = multiplexerLock.withLock {
            fetchers.getOrPut(key) {
                // TODO
                //  we need cleanup here
                Multiplexer(
                    scope = scope,
                    bufferSize = 0,
                    source = {
                        realFetcher(key).onEach {
                            sourceOfTruth.write(key, it)
                        }
                    }
                )
            }
        }
        return multiplexer.create()
    }

    class ManagedFetcherSubscription<Key, Input>(
        private val scope: CoroutineScope,
        private val key: Key,
        private val src: (Key) -> Flow<Input>
    ) {
        private var job: Job? = null
        private val jobLock = Mutex()
        private val _firstValue = CompletableDeferred<Input>()
        val firstValue
            get() : Deferred<Input> = _firstValue
        suspend fun start() {
            val theJob = jobLock.withLock {
                job ?: launch().also {
                    job = it
                }
            }
            theJob.start()
        }


        private fun launch(): Job {
            return scope.launch(start = CoroutineStart.LAZY) {
                src(key).catch {
                    _firstValue.completeExceptionally(it)
                }.collect {
                    _firstValue.complete(it)
                }
            }
        }

        suspend fun stop() {
            val toBeCancelled = jobLock.withLock {
                job.also {
                    job = null
                }
            }
            toBeCancelled?.cancel()
        }
    }
}