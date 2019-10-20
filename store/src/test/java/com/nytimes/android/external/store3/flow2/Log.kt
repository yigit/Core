package com.nytimes.android.external.store3.flow2

fun log(msg:Any?) {
    println("[${Thread.currentThread().name}]: $msg")
}