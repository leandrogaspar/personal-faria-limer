package lgs.publisher_consumer.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import lgs.configuration.Databases
import lgs.configuration.suspendedTransaction
import lgs.publisher_consumer.Producer
import org.jetbrains.exposed.sql.insert
import java.time.Clock
import java.time.Instant

@Singleton
class SqlBasedProducer(
    private val clock: Clock,
    private val db: Databases,
) : Producer {
    override suspend fun produceMessage(topic: String, payload: String) {
        return suspendedTransaction(Dispatchers.IO, db.writer) {
            MessageTable.insert {
                it[this.topic] = topic
                it[this.sentAt] = nowAsEpochMilli()
                it[this.payload] = payload
            }
        }
    }

    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}