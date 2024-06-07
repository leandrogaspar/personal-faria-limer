package lgs.l3.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import lgs.l3.L3
import lgs.l3.model.Item
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.api.ExposedBlob
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Clock
import java.time.Instant

@Singleton
class SqlBasedL3(
    private val clock: Clock,
    private val db: Database,
) : L3 {
    override suspend fun putItem(key: String, content: ByteArray): Item {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            val existingItem = ItemTable.selectAll()
                .where { ItemTable.key eq key }
                .orderBy(ItemTable.version to SortOrder.DESC)
                .limit(1)
                .firstOrNull()?.item()

            val newVersion = existingItem?.version?.inc() ?: 1
            ItemTable.insert {
                it[this.key] = key
                it[this.insertedAt] = nowAsEpochMilli()
                it[this.version] = newVersion
                it[this.content] = ExposedBlob(content)
            }

            ItemTable.selectAll()
                .where { (ItemTable.key eq key) and (ItemTable.version eq newVersion) }
                .limit(1)
                .first().item()
        }
    }

    override suspend fun getItem(key: String, version: Int?): Item? {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            val query = when (version) {
                null -> ItemTable.selectAll()
                    .where { ItemTable.key eq key }
                    .orderBy(ItemTable.version to SortOrder.DESC)
                    .limit(1)

                else -> ItemTable.selectAll()
                    .where { (ItemTable.key eq key) and (ItemTable.version eq version) }
                    .orderBy(ItemTable.version to SortOrder.DESC)
                    .limit(1)
            }
            query.firstOrNull()?.item()
        }
    }

    override suspend fun deleteItem(key: String): Item? {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            val existingItem = ItemTable.selectAll()
                .where { ItemTable.key eq key }
                .orderBy(ItemTable.version to SortOrder.DESC)
                .limit(1)
                .firstOrNull()?.item() ?: return@newSuspendedTransaction null

            ItemTable.update({ (ItemTable.key eq key) and (ItemTable.version eq existingItem.version) }) {
                it[this.deletedAt] = nowAsEpochMilli()
            }

            ItemTable.selectAll()
                .where { (ItemTable.key eq key) and (ItemTable.version eq existingItem.version) }
                .limit(1)
                .first().item()
        }
    }

    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}