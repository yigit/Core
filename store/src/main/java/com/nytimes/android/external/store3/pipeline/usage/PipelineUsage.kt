package com.nytimes.android.external.store3.pipeline.usage

import com.nytimes.android.external.store3.base.impl.BarCode
import com.nytimes.android.external.store3.pipeline.beginPipeline
import com.nytimes.android.external.store3.pipeline.withCache
import com.nytimes.android.external.store3.pipeline.withConverter
import com.nytimes.android.external.store3.pipeline.withPersister
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flow

class TaskDTO(val id: String, val name: String)
class Task(val id: String, val name: String)
class UITask(val id: String, val name: String)

object Api {
    fun get(barCode: BarCode): TaskDTO = TODO()
}

object Db {
    fun get(barCode: BarCode): UITask = TODO()
    fun put(barCode: BarCode, task: Task): Unit = TODO()
}

object ResponseParser {
    fun convertResponse(taskDTO: TaskDTO): Task = TODO()
}

@FlowPreview
val pipeline = beginPipeline { barCode: BarCode ->
    flow {
        emit(Api.get(barCode))
    }
}.withConverter {
    ResponseParser.convertResponse(it)
}.withCache()
        .withPersister(
                reader = { code ->
                    flow {
                        emit(Db.get(code))
                    }
                },
                writer = { key, value ->
                    Db.put(key, value)
                }
        )

@FlowPreview
suspend fun usage() {
    val task = pipeline.stream(BarCode(type = "foo", key = "bar"))
}