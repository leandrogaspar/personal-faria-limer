package lgs.l3

import java.time.Instant

data class Item(
    val key: String,
    val version: Int,
    val insertedAt: Instant,
    val deletedAt: Instant? = null,
    val content: ByteArray?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Item

        if (key != other.key) return false
        if (version != other.version) return false
        if (insertedAt != other.insertedAt) return false
        if (deletedAt != other.deletedAt) return false
        return content.contentEquals(other.content)
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + version
        result = 31 * result + insertedAt.hashCode()
        result = 31 * result + (deletedAt?.hashCode() ?: 0)
        result = 31 * result + content.contentHashCode()
        return result
    }
}