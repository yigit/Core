package com.nytimes.android.external.store3.pipeline

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import org.assertj.core.api.Assertions

internal class FlowingFakeFetcher<Key, Output>(
    vararg val responses: Pair<Key, Output>
) {
    fun createFlow(key: Key) = flow {
        responses.filter {
            it.first == key
        }.forEach {
            emit(it.second)
            delay(1)
        }
    }
}

internal class FakeFetcher<Key, Output>(
    vararg val responses: Pair<Key, Output>
) {
    internal var index = 0
    @Suppress("RedundantSuspendModifier") // needed for function reference
    suspend fun fetch(key: Key): Output {
        if (index >= responses.size) {
            throw AssertionError("unexpected fetch request")
        }
        val pair = responses[index++]
        Assertions.assertThat(pair.first).isEqualTo(key)
        return pair.second
    }
}

internal class InMemoryPersister<Key, Output> {
    internal val data = mutableMapOf<Key, Output>()

    @Suppress("RedundantSuspendModifier")// for function reference
    suspend fun read(key: Key) = data[key]

    @Suppress("RedundantSuspendModifier") // for function reference
    suspend fun write(key: Key, output: Output) {
        data[key] = output
    }

    suspend fun asObservable() = SimplePersisterAsFlowable(
        reader = this::read,
        writer = this::write
    )
}

/**
 * Asserts only the [expected] items by just taking that many from the stream
 *
 * Use this when Pipeline has an infinite part (e.g. Persister or a never ending fetcher)
 */
internal suspend fun <T> Flow<T>.assertItems(vararg expected: T) {
    Assertions.assertThat(this.take(expected.size).toList())
        .isEqualTo(expected.toList())
}

/**
 * Takes all elements from the stream and asserts them.
 * Use this if test does not have an infinite flow (e.g. no persister or no infinite fetcher)
 */
internal suspend fun <T> Flow<T>.assertCompleteStream(vararg expected: T) {
    Assertions.assertThat(this.toList())
        .isEqualTo(expected.toList())
}