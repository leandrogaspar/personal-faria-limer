package lgs.machado.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import lgs.configuration.suspendedTransaction
import lgs.machado.Producer
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insert
import java.time.Clock
import java.time.Instant

@Singleton
class SqlBasedProducer(
    private val clock: Clock,
    private val db: Database,
) : Producer {
    override suspend fun produceMessage(topic: String, payload: String) {
        return suspendedTransaction(Dispatchers.IO, db) {
            MessageTable.insert {
                it[this.topic] = topic
                it[this.sentAt] = nowAsEpochMilli()
                it[this.payload] = payload
            }
        }
    }

    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}