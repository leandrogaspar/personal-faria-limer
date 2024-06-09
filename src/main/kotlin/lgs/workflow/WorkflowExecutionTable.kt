package lgs.workflow

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import lgs.publisher_consumer.Message
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import java.time.Instant
import java.util.*

object UUIDSerializer : KSerializer<UUID> {
    override val descriptor = PrimitiveSerialDescriptor("UUID", PrimitiveKind.STRING)
    override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())
    override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
}

@Serializable
data class WorkflowExecution(
    @Serializable(with = UUIDSerializer::class) val id: UUID,
    val state: String,
)

object WorkflowExecutionTable : Table() {
    val id = uuid("id").uniqueIndex()
    val state = varchar("state", 255)
}

fun ResultRow.workflowExecution() = WorkflowExecution(
    id = this[WorkflowExecutionTable.id],
    state = this[WorkflowExecutionTable.state],
)
