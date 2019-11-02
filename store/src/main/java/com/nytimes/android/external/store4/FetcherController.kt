package com.nytimes.android.external.store4

import com.nytimes.android.external.store4.multiplex.Multiplexer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * This class maintains one and only 1 fetcher for a given [Key].
 */
@FlowPreview
@ExperimentalCoroutinesApi
internal class FetcherController<Key, Input, Output>(
    private val scope: CoroutineScope,
    private val realFetcher: (Key) -> Flow<Input>,
    private val sourceOfTruth: SourceOfTruthWithBarrier<Key, Input, Output>?
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
                            sourceOfTruth?.write(key, it)
                        }
                    }
                )
            }
        }
        return multiplexer.create()
    }
}