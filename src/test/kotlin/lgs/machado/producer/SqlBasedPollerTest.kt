package lgs.machado.producer

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import lgs.configuration.DatabaseFactory
import lgs.machado.Consumer
import lgs.machado.model.Message
import lgs.machado.sql_based.SqlBasedProducer
import lgs.test_helpers.cleanDbFile
import lgs.test_helpers.createTestClock
import lgs.test_helpers.randomString
import kotlin.time.Duration.Companion.seconds

class SqlBasedPollerTest(
): ShouldSpec() {
    private val dbFile = "./sql_based_poller.db"
    private val db by lazy {
        cleanDbFile(dbFile)
        DatabaseFactory().database(dbFile)
    }
    private val clock by lazy { createTestClock() }
    private val publisher by lazy { SqlBasedProducer(db = db, clock = clock) }
    private val poller by lazy { SqlBasedMachado(db = db, clock = clock) }

    override fun afterSpec(f: suspend (Spec) -> Unit) {
        super.afterSpec(f)
        cleanDbFile(dbFile)
    }

    init {
        context("pollMessages") {
            val consumer = object: Consumer {
                override fun group() = "pollMessagesGroup"
                override fun topic() = "pollMessagesTopic"
                override suspend fun consumeMessages(messages: List<Message>) {}
            }
            val firstMessage = randomString()
            val secondMessage = randomString()
            val thirdMessage = randomString()
            publisher.produceMessage(consumer.topic(), firstMessage)
            clock.plus(10.seconds)
            publisher.produceMessage(consumer.topic(), secondMessage)
            clock.plus(10.seconds)
            publisher.produceMessage(consumer.topic(), thirdMessage)

            should("retrieve consumer's pending messages prioritizing older messages up to maxPollSize") {
                val pollOne = poller.pollMessages(consumer, 1)
                pollOne.size shouldBe 1
                pollOne[0].payload shouldBe firstMessage
            }

            should("retrieve all consumer's pending messages prioritizing older messages") {
                val pollAll = poller.pollMessages(consumer, 10)
                pollAll.size shouldBe 3
                pollAll[0].payload shouldBe firstMessage
                pollAll[1].payload shouldBe secondMessage
                pollAll[2].payload shouldBe thirdMessage
            }
        }

        context("markMessagesAsConsumed") {
            val consumer = object: Consumer {
                override fun group() = "markMessagesAsConsumedGroupA"
                override fun topic() = "markMessagesAsConsumedTopicA"
                override suspend fun consumeMessages(messages: List<Message>) {}
            }
            should("prevent given messages to be polled again from a consumer") {
                repeat(5) {
                    publisher.produceMessage(consumer.topic(), randomString())
                }
                val messages = poller.pollMessages(consumer, 10)
                poller.markMessagesAsConsumed(consumer, messages.slice(0..2))
                val remainingMessages = poller.pollMessages(consumer, 10)
                remainingMessages.size shouldBe 2
                remainingMessages shouldBe messages.slice(3..4)
            }
        }
    }
}
