package lgs.workflow

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import lgs.configuration.Databases
import lgs.configuration.suspendedTransaction
import lgs.object_storage.sql_based.item
import lgs.publisher_consumer.Producer
import org.jetbrains.exposed.sql.*
import java.util.*

data class StepResponse(val result: String, val contextDelta: Map<String,String>? = null)

data class On(
    val result: String,
    val goTo: String,
)


interface ActionHandler {
    operator fun invoke(context: Map<String,String>): StepResponse
}

class MyAction : ActionHandler {
    override fun invoke(context: Map<String,String>): StepResponse {
        println("custom action")
        return StepResponse(result = "success")
    }
}

val actions = mapOf<String, ActionHandler>(
    MyAction::class.qualifiedName!! to MyAction()
)

@Serializable
data class Step(
    val name: String,
    val action: String,
    val on: Map<String, String>,
    val maxVisits: Int? = 1
)

val workflow = listOf(
    Step(
        name = "Start",
        action = MyAction::class.qualifiedName!!,
        on = mapOf(
            "success" to "FirstStep"
        )
    ),
    Step(
        name = "FirstStep",
        action = MyAction::class.qualifiedName!!,
        on = mapOf(
            "success" to "SecondStep"
        )
    ),
    Step(
        name = "SecondStep",
        action = MyAction::class.qualifiedName!!,
        on = mapOf(
            "success" to "FinalStep"
        )
    ),
    Step(
        name = "FinalStep",
        action = MyAction::class.qualifiedName!!,
        on = emptyMap()
    )
)

data class Workflow(
    val name: String,
    val dag: List<Step>,
)

@Singleton
class WorkflowManager(
    private val dbs: Databases,
    private val producer: Producer,
) {
    suspend fun startWorkflow(
        id: UUID,
        workflow: Workflow,
        context: Map<String, String> = emptyMap()
    ) {
        suspendedTransaction(Dispatchers.IO, dbs.writer) {
            addLogger(StdOutSqlLogger)
            val workflowExecution = WorkflowExecutionTable.insertIgnore {
                it[this.id] = id
                it[this.state] = "start"
            }.resultedValues?.firstOrNull()?.workflowExecution()
            workflowExecution?.let {
                producer.produceMessage("test", Json.encodeToString(WorkflowExecution.serializer(), it))
            }
        }
    }
}


fun main() {
    println(Json.encodeToString(workflow))
    val stepMap = workflow.associateBy { it.name }
    var step = stepMap["Start"]!!
    val context = mapOf("a" to "b")
    while (true) {
        val action = actions[step.action]
        val response = action!!.invoke(context)
        println(response)
        if (step.on.isEmpty()) {
            println("done")
            break;
        }
        val nextStep = step.on[response.result]
        step = stepMap[nextStep]!!
    }
}