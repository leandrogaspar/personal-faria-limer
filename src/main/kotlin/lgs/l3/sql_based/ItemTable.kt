package lgs.l3.sql_based

import lgs.l3.Item
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import java.time.Instant

object ItemTable : Table() {
    val folder = varchar("folder", 1000).index()
    val key = varchar("item_key", 1000).index()
    val insertedAt = long("inserted_at")
    val deletedAt = long("deleted_at").nullable()
    val version = integer("version").default(1)
    val content = blob("content")

    override val primaryKey = PrimaryKey(folder, key, version, name = "PK_Item")
}

fun ResultRow.item() = Item(
    folder = this[ItemTable.folder],
    key = this[ItemTable.key],
    insertedAt = Instant.ofEpochMilli(this[ItemTable.insertedAt]),
    deletedAt = this[ItemTable.deletedAt]?.let { Instant.ofEpochMilli(it) },
    version = this[ItemTable.version],
    content = this[ItemTable.content].bytes,
)