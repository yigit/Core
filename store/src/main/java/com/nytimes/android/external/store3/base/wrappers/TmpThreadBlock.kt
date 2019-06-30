package com.nytimes.android.external.store3.base.wrappers

import java.util.concurrent.Semaphore

object TmpThreadBlock {
    val semaphore = Semaphore(1)

    fun lock() {
        semaphore.acquire()
    }

    fun unlock() {
        semaphore.release()
    }
}