package lgs.l3.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import lgs.configuration.Databases
import lgs.configuration.suspendedTransaction
import lgs.l3.L3
import lgs.l3.model.Item
import lgs.l3.model.toItemEvent
import lgs.machado.Producer
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.api.ExposedBlob
import java.time.Clock
import java.time.Instant

@Singleton
class SqlBasedL3(
    private val clock: Clock,
    private val db: Databases,
    private val producer: Producer,
) : L3 {
    override suspend fun putItem(folder: String, key: String, content: ByteArray): Item {
        return suspendedTransaction(Dispatchers.IO, db.writer) {
            val existingItem = ItemTable.selectAll()
                .where { (ItemTable.folder eq folder) and (ItemTable.key eq key) }
                .orderBy(ItemTable.version to SortOrder.DESC)
                .limit(1)
                .firstOrNull()?.item()

            val newVersion = existingItem?.version?.inc() ?: 1
            ItemTable.insert {
                it[this.folder] = folder
                it[this.key] = key
                it[this.insertedAt] = nowAsEpochMilli()
                it[this.version] = newVersion
                it[this.content] = ExposedBlob(content)
            }

            val item = ItemTable.selectAll()
                .where { (ItemTable.folder eq folder) and (ItemTable.key eq key) and (ItemTable.version eq newVersion) }
                .limit(1)
                .first().item()
            producer.produceMessage(topicForItem(item), Json.encodeToString(item.toItemEvent()))
            item
        }
    }

    override suspend fun getItem(folder: String, key: String, version: Int?): Item? {
        return suspendedTransaction(Dispatchers.IO, db.reader) {
            val query = when (version) {
                null -> ItemTable.selectAll()
                    .where { (ItemTable.folder eq folder) and (ItemTable.key eq key) }
                    .orderBy(ItemTable.version to SortOrder.DESC)
                    .limit(1)

                else -> ItemTable.selectAll()
                    .where { (ItemTable.folder eq folder) and (ItemTable.key eq key) and (ItemTable.version eq version) }
                    .orderBy(ItemTable.version to SortOrder.DESC)
                    .limit(1)
            }
            query.firstOrNull()?.item()
        }
    }

    override suspend fun deleteItem(folder: String, key: String): Item? {
        return suspendedTransaction(Dispatchers.IO, db.writer) {
            val existingItem = ItemTable.selectAll()
                .where { (ItemTable.folder eq folder) and (ItemTable.key eq key) }
                .orderBy(ItemTable.version to SortOrder.DESC)
                .limit(1)
                .firstOrNull()?.item() ?: return@suspendedTransaction null

            ItemTable.update({
                (ItemTable.folder eq folder) and
                        (ItemTable.key eq key) and
                        (ItemTable.version eq existingItem.version)
            }) {
                it[this.deletedAt] = nowAsEpochMilli()
            }

            val item = ItemTable.selectAll()
                .where {
                    (ItemTable.folder eq folder) and
                            (ItemTable.key eq key) and
                            (ItemTable.version eq existingItem.version)
                }
                .limit(1)
                .first().item()
            producer.produceMessage(topicForItem(item), Json.encodeToString(item.toItemEvent()))
            item
        }
    }

    private fun topicForItem(item: Item): String = "l3-ie-${item.folder}"
    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}