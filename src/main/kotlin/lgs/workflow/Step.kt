package lgs.workflow

data class Context(val data: Map<String, String>)

data class StepResponse(val result: String, val contextDelta: Context? = null)

data class On(
    val result: String,
    val goTo: String,
)

fun interface Action {
    operator fun invoke(context: Context): StepResponse
}

class MyAction : Action {
    override fun invoke(context: Context): StepResponse {
        println("custom action")
        return StepResponse(result = "success")
    }
}

data class Step(
    val name: String,
    val action: Action,
    val on: Map<String, String>,
    val maxVisits: Int? = 1
)

val workflow = listOf(
    Step(
        name = "Start",
        action = MyAction(),
        on = mapOf(
            "success" to "FirstStep"
        )
    ),
    Step(
        name = "FirstStep",
        action = fun(context: Context): StepResponse {
            println("FirstStep $context")
            return StepResponse(result = "success")
        },
        on = mapOf(
            "success" to "SecondStep"
        )
    ),
    Step(
        name = "SecondStep",
        action = fun(context: Context): StepResponse {
            println("SecondStep $context")
            return StepResponse(result = "success")
        },
        on = mapOf(
            "success" to "FinalStep"
        )
    ),
    Step(
        name = "FinalStep",
        action = fun(context: Context): StepResponse {
            println("FinalStep $context")
            return StepResponse(result = "success")
        },
        on = emptyMap()
    )
)

fun main() {
    val stepMap = workflow.associateBy { it.name }
    var step = stepMap["Start"]!!
    val context = Context(mapOf("a" to "b"))
    while (true) {
        val response = step.action(context)
        println(response)
        if (step.on.isEmpty()) {
            println("done")
            break;
        }
        val nextStep = step.on[response.result]
        step = stepMap[nextStep]!!
    }
}