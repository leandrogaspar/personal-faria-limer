package lgs.machado

import lgs.machado.model.Message
import java.util.*

data class ConsumeFailure(
    val failedMessageId: UUID,
    val cause: String,
)

interface Consumer {
    fun group(): String
    fun topic(): String
    suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure>
}
