@file:UseSerializers(InstantSerializer::class)

package lgs.l3.model

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.Instant

@Serializable
data class ItemEvent(
    val folder: String,
    val key: String,
    val version: Int,
    val insertedAt: Instant,
    val deletedAt: Instant? = null,
)

object InstantSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("java.time.Instant", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Instant = Instant.parse(decoder.decodeString())
}

fun Item.toItemEvent() = ItemEvent(
    folder = this.folder,
    key = this.key,
    version = this.version,
    insertedAt = this.insertedAt,
    deletedAt = this.deletedAt,
)
