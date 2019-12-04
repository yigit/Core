package com.nytimes.android.external.store3.util

@Deprecated("just for testing")
interface KeyParser<in Key, in Raw, out Parsed> {

    @Throws(ParserException::class)
    suspend fun apply(key: Key, raw: Raw): Parsed
}
