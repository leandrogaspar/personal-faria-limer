package lgs.machado.sql_based

import kotlinx.coroutines.Dispatchers
import lgs.machado.Consumer
import lgs.machado.core.ConsumerManager
import lgs.machado.model.Message
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Instant
import java.util.*

class SqlBasedConsumerManager(
    private val clock: Clock,
    private val db: Database,
) : ConsumerManager {
    private val logger: Logger = LoggerFactory.getLogger(SqlBasedConsumerManager::class.java)

    override suspend fun consumeMessages(consumer: Consumer, maxPollSize: Int) {
        val messages = pollMessages(consumer, maxPollSize)

        // Todo: DLQ failures eventually?
        val failures = consumer.consumeMessages(messages)
        logger.debug("Failed to consume ${failures.size} messages on topic ${consumer.topic()} for group ${consumer.group()}")
        val failedMessageIds = failures.map { it.failedMessageId }
        val successes = messages.map { it.id }
            .filter { failedMessageIds.contains(it)}

        markMessagesAsConsumed(consumer, successes)
    }

    private suspend fun pollMessages(consumer: Consumer, maxPollSize: Int): Set<Message> {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            MessageTable
                .join(ConsumerTable, JoinType.LEFT) {
                    (MessageTable.id eq ConsumerTable.messageId) and
                            (MessageTable.topic eq consumer.topic()) and
                            (ConsumerTable.topic eq consumer.topic()) and
                            (ConsumerTable.group eq consumer.group())
                }
                .selectAll()
                .where { ConsumerTable.consumedAt.isNull() }
                .orderBy(MessageTable.sentAt to SortOrder.ASC)
                .limit(maxPollSize)
                .map { it.message() }
                .toSet()
        }
    }

    private suspend fun markMessagesAsConsumed(consumer: Consumer, messageIds: List<UUID>) {
        return newSuspendedTransaction(Dispatchers.IO, db) {
            ConsumerTable.batchInsert(messageIds) { messageId ->
                this[ConsumerTable.group] = consumer.group()
                this[ConsumerTable.topic] = consumer.topic()
                this[ConsumerTable.messageId] = messageId
                this[ConsumerTable.consumedAt] = nowAsEpochMilli()
            }
        }
    }

    private fun nowAsEpochMilli() = Instant.now(clock).toEpochMilli()
}