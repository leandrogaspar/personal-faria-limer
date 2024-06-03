package lgs.machado.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import lgs.machado.Consumer
import lgs.machado.core.Poller
import lgs.machado.model.Message
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Instant

@Singleton
class SqlBasedPoller(
    private val clock: Clock,
    private val db: Database,
): Poller {
    private val logger: Logger = LoggerFactory.getLogger(SqlBasedPoller::class.java)

    override suspend fun pollMessages(consumer: Consumer, maxPollSize: Int): List<Message> {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            MessageTable
                .join(GroupConsumeTable, JoinType.LEFT) {
                    (MessageTable.id eq GroupConsumeTable.messageId) and
                            (MessageTable.topic eq consumer.consumerTopic()) and
                            (GroupConsumeTable.topic eq consumer.consumerTopic()) and
                            (GroupConsumeTable.consumerGroup eq consumer.consumerGroup())
                }
                .selectAll()
                .where { GroupConsumeTable.consumedAt.isNull() }
                .orderBy(MessageTable.sentAt to SortOrder.ASC)
                .limit(maxPollSize)
                .map { it.message() }
        }
    }

    override suspend fun markMessagesAsConsumed(consumer: Consumer, messages: List<Message>) {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            GroupConsumeTable.batchInsert(messages) { message ->
                this[GroupConsumeTable.consumerGroup] = consumer.consumerGroup()
                this[GroupConsumeTable.topic] = message.topic
                this[GroupConsumeTable.messageId] = message.id
                this[GroupConsumeTable.consumedAt] = nowAsEpochMilli()
            }
        }
    }

    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}