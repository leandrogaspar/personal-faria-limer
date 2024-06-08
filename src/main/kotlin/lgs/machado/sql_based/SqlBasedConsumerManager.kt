package lgs.machado.sql_based

import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import lgs.configuration.Databases
import lgs.configuration.suspendedTransaction
import lgs.machado.Consumer
import lgs.machado.core.ConsumerManager
import lgs.machado.model.Message
import org.jetbrains.exposed.sql.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Instant
import java.util.*

@Singleton
class SqlBasedConsumerManager(
    private val clock: Clock,
    private val db: Databases,
) : ConsumerManager {
    private val logger: Logger = LoggerFactory.getLogger(SqlBasedConsumerManager::class.java)

    override suspend fun consumeMessages(consumer: Consumer, maxPollSize: Int) {
        val messages = pollMessages(consumer, maxPollSize)
        if (messages.isEmpty()) {
            return
        }

        // Todo: DLQ failures eventually?
        val failures = consumer.consumeMessages(messages)
        logger.debug("Failed to consume ${failures.size} messages on topic ${consumer.topic()} for group ${consumer.group()}")
        val failedMessageIds = failures.map { it.failedMessageId }
        val successes = messages.map { it.id }
            .filter { !failedMessageIds.contains(it) }

        markMessagesAsConsumed(consumer, successes)
    }

    private suspend fun pollMessages(consumer: Consumer, maxPollSize: Int): List<Message> {
        return suspendedTransaction(Dispatchers.IO, db.reader) {
            MessageTable
                .join(ConsumerTable, JoinType.LEFT) {
                    (MessageTable.id eq ConsumerTable.messageId) and
                            (ConsumerTable.topic eq consumer.topic()) and
                            (ConsumerTable.group eq consumer.group())
                }
                .selectAll()
                .where { MessageTable.topic eq consumer.topic() }
                .andWhere { ConsumerTable.consumedAt.isNull() }
                .orderBy(MessageTable.sentAt to SortOrder.ASC)
                .limit(maxPollSize)
                .map { it.message() }
        }
    }

    private suspend fun markMessagesAsConsumed(consumer: Consumer, messageIds: List<UUID>) {
        return suspendedTransaction(Dispatchers.IO, db.writer) {
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