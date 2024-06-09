package lgs.publisher_consumer.sql_based

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import lgs.configuration.DatabaseFactory
import lgs.configuration.Databases
import lgs.publisher_consumer.ConsumeFailure
import lgs.publisher_consumer.Consumer
import lgs.publisher_consumer.Message
import lgs.test_helpers.cleanDbFile
import lgs.test_helpers.createTestClock
import lgs.test_helpers.createTrackingConsumer
import lgs.test_helpers.randomString
import kotlin.time.Duration.Companion.seconds

class SqlBasedConsumerManagerTest(
) : ShouldSpec() {
    private val dbFile = "./dbs/sql_based_consumer_manager.db"
    private val db by lazy {
        cleanDbFile(dbFile)
        DatabaseFactory().createDatabase(dbFile)
    }
    private val clock by lazy { createTestClock() }
    private val publisher by lazy { SqlBasedProducer(db = Databases(db, db), clock = clock) }
    private val consumerManager by lazy { SqlBasedConsumerManager(db = Databases(db, db), clock = clock) }

    init {
        context("consumeMessages") {
            should("consume pending messages prioritizing older messages up to maxPollSize") {
                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(consumedMessages)
                val sentMessages = sendMessages(consumer.topic, 3)

                consumerManager.consumeMessages(consumer, 1)
                consumedMessages.size shouldBe 1
                consumedMessages[0].payload shouldBe sentMessages[0]
            }

            should("consume all pending messages prioritizing older messages") {
                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(consumedMessages)
                val sentMessages = sendMessages(consumer.topic, 3)

                consumerManager.consumeMessages(consumer, 10)
                consumedMessages.size shouldBe 3
                consumedMessages[0].payload shouldBe sentMessages[0]
                consumedMessages[1].payload shouldBe sentMessages[1]
                consumedMessages[2].payload shouldBe sentMessages[2]
            }

            should("do nothing if there are no pending messages") {
                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(consumedMessages)
                consumerManager.consumeMessages(consumer, 10)
                consumedMessages.isEmpty() shouldBe true
            }

            should("keep failures available for consume") {
                val shouldFailMessage = "should_fail"
                val messagesReceivedOnConsumer = mutableListOf<Message>()
                val consumer = object : Consumer(randomString(), randomString()) {
                    override suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure> {
                        messagesReceivedOnConsumer.addAll(messages)
                        return messages.filter { it.payload == shouldFailMessage }
                            .map { ConsumeFailure(it.id, "It failed!") }
                    }
                }
                publisher.produceMessage(consumer.topic, shouldFailMessage)
                sendMessages(consumer.topic, 4)

                // The first poll, we get the message that should fail + 4 that should pass
                consumerManager.consumeMessages(consumer, 10)
                messagesReceivedOnConsumer.size shouldBe 5
                messagesReceivedOnConsumer.clear()

                // On future polls, as the 4 passing are marked as consume, we only ge the failing
                consumerManager.consumeMessages(consumer, 10)
                messagesReceivedOnConsumer.size shouldBe 1
                messagesReceivedOnConsumer[0].payload shouldBe shouldFailMessage
            }

            should("consume messages from a topic independently on consumers from different groups") {
                val topic = randomString()
                val groupA = randomString() + "group-a"
                val groupB = randomString() + "group-b"

                val consumedMessagesOnGroupA = mutableListOf<Message>()
                val consumerFromGroupA = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessagesOnGroupA,
                    group = groupA,
                    topic = topic,
                )

                val consumedMessagesOnGroupB = mutableListOf<Message>()
                val consumerFromGroupB = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessagesOnGroupB,
                    group = groupB,
                    topic = topic,
                )

                sendMessages(topic, 3)
                consumerManager.consumeMessages(consumerFromGroupA, 10)
                consumerManager.consumeMessages(consumerFromGroupB, 10)
                consumedMessagesOnGroupA.size shouldBe 3
                consumedMessagesOnGroupB.size shouldBe 3
                consumedMessagesOnGroupA shouldBe consumedMessagesOnGroupB
            }

            should("consume messages from a topic together on consumers within same group") {
                val topic = randomString()
                val group = randomString()

                val consumedMessagesOnConsumerA = mutableListOf<Message>()
                val consumerA = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessagesOnConsumerA,
                    group = group,
                    topic = topic,
                )

                val consumedMessagesOnConsumerB = mutableListOf<Message>()
                val consumerB = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessagesOnConsumerB,
                    group = group,
                    topic = topic,
                )

                val sentMessages = sendMessages(topic, 3)
                consumerManager.consumeMessages(consumerA, 1)
                consumerManager.consumeMessages(consumerB, 10)
                consumedMessagesOnConsumerA.size shouldBe 1
                consumedMessagesOnConsumerA[0].payload shouldBe sentMessages[0]
                consumedMessagesOnConsumerB.size shouldBe 2
                consumedMessagesOnConsumerB[0].payload shouldBe sentMessages[1]
                consumedMessagesOnConsumerB[1].payload shouldBe sentMessages[2]
            }
        }
    }

    private suspend fun sendMessages(topic: String, n: Int): List<String> {
        val sentMessages = mutableListOf<String>()
        repeat(n) {
            clock.plus(10.seconds)
            val message = randomString()
            publisher.produceMessage(topic, message)
            sentMessages.add(message)
        }
        return sentMessages
    }
}
