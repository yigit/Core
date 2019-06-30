package com.birbit.comnytimesandroidexternalstore3.cache

import com.nytimes.android.external.cache3.CacheBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.ActorScope
import kotlinx.coroutines.channels.actor

internal class StoreCacheImpl<K, V> : StoreCache<K, V> {
    private val realCache = CacheBuilder.newBuilder().build<K, StoreRecord<K, V>>()

    override suspend fun get(key: K, loader: Loader<K, V>): V {
        return realCache.get(key) {
            StoreRecord(key, loader)
        }.value()
    }

    override suspend fun put(key: K, value: V) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override suspend fun invalidate(key: K) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override suspend fun clearAll() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override suspend fun getIfPresent(key: K): V? {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}