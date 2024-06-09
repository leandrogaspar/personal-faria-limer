package lgs.publisher_consumer

import java.time.Instant
import java.util.*

data class Message(
    val id: UUID,
    val topic: String,
    val sentAt: Instant,
    val payload: String,
)