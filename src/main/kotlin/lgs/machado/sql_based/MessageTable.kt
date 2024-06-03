package lgs.machado.sql_based

import lgs.machado.model.Message
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import java.time.Instant
import java.util.*

object MessageTable: Table() {
    val id = uuid("id").clientDefault { UUID.randomUUID() }.uniqueIndex()
    val topic = varchar("topic", 255)
    val sentAt = long("sent_at")
    val payload = text("payload")
}

fun ResultRow.message() = Message(
    id = this[MessageTable.id],
    topic = this[MessageTable.topic],
    sentAt = Instant.ofEpochMilli(this[MessageTable.sentAt]),
    payload = this[MessageTable.payload],
)
