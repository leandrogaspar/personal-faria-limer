package lgs.machado.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import lgs.machado.Producer
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Clock
import java.time.Instant

@Singleton
class SqlBasedPublisher(
    private val clock: Clock,
    private val db: Database,
) : Producer {
    override suspend fun send(topic: String, payload: String) {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            MessageTable.insert {
                it[this.topic] = topic
                it[this.sentAt] = nowAsEpochMilli()
                it[this.payload] = payload
            }
        }
    }

    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}