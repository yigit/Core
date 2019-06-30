package com.nytimes.android.external.store3

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.base.impl.MemoryPolicy
import com.nytimes.android.external.store3.base.impl.Store
import com.nytimes.android.external.store3.base.wrappers.MemoryCacheStore
import com.nytimes.android.external.store3.base.wrappers.Store
import com.nytimes.android.external.store3.base.wrappers.TmpThreadBlock
import junit.framework.Assert.assertEquals
import junit.framework.Assert.fail
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.junit.Test
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean


class DontCacheErrorsTest {

    private var shouldThrow = AtomicBoolean(false)
    private var firstRequest = Mutex(true)
    private val store: Store<Int, BarCode> = Store {
        if (shouldThrow.get()) {
            firstRequest.unlock()
            throw RuntimeException()
        } else {
            0
        }
    }

    private val store2 = MemoryCacheStore(store, MemoryPolicy(
            expireAfterAccess = 10,
            expireAfterWrite = 10,
            expireAfterTimeUnit = TimeUnit.DAYS,
            maxSizeNotDefault = 10
    ))

    @Test
    fun testStoreDoesntCacheErrors() = runBlocking<Unit> {
        val barcode = BarCode("bar", "code")

        shouldThrow.set(true)

        try {
            store.get(barcode)
            fail()
        } catch (e: RuntimeException) {
            e.printStackTrace()
        }

        shouldThrow.set(false)
        store.get(barcode)
    }

    @Test
    fun multiThreaded() = runBlocking<Unit> {
        val barcode = BarCode("bar", "code")
        // uncomment and test will fail
        //TmpThreadBlock.lock()
        shouldThrow.set(true)
        val get1 = GlobalScope.async(Dispatchers.IO) {
            try {
                store2.get(barcode)
                fail()
                true
            } catch (e: RuntimeException) {
                e.printStackTrace()
                false
            }
        }
        firstRequest.lock()
        delay(1000)
        shouldThrow.set(false)
        val get2 = GlobalScope.async(Dispatchers.IO) {
            store2.get(barcode)
        }
        TmpThreadBlock.unlock()
        assertEquals(get1.await(), false)
        assertEquals(get2.await(), 0)
    }
}
