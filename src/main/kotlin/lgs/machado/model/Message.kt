package lgs.machado.model

import java.time.Instant
import java.util.*

data class Message(
    val id: UUID,
    val topic: String,
    val sentAt: Instant,
    val payload: String,
)