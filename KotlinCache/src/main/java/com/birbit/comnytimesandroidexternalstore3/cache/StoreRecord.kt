package com.birbit.comnytimesandroidexternalstore3.cache

import com.nytimes.android.external.cache3.ExecutionError
import com.nytimes.android.external.cache3.UncheckedExecutionException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.lang.Error
import java.lang.IllegalStateException
import java.lang.RuntimeException
import java.util.concurrent.ExecutionException
import kotlin.coroutines.coroutineContext

internal class StoreRecord<K, V>(
        private val key: K,
        loader: Loader<K, V>
) {
    private var inFlight = Mutex(false)
    private var _loader: Loader<K, V>? = loader
    @Volatile
    private var _value: Result<V>? = null
    @Throws(ExecutionException::class, ExecutionError::class, UncheckedExecutionException::class)
    suspend fun value(): V {
        val res = _value
        if (res != null) {
            return res.getOrThrow()
        }
        return inFlight.withLock {
            val loader = _loader
                    ?: throw IllegalStateException("inconsistency, loader should not be null")
            try {
                Result.success(loader(key))
            } catch (throwable: Throwable) {
                Result.failure<V>(recoverException(throwable))
            }

        }.also {
            _value = it
        }.getOrThrow()
    }

    private fun recoverException(throwable: Throwable?): Throwable {
        return when (throwable) {
            is Exception -> ExecutionException(throwable)
            is Error -> ExecutionError(throwable)
            else -> UncheckedExecutionException(throwable)
        }
    }
}
