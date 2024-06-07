package lgs.machado

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import kotlinx.coroutines.delay
import lgs.machado.model.Message
import lgs.test_helpers.createTrackingConsumer
import lgs.test_helpers.randomString

@MicronautTest
class MachadoTest(
    private val producer: Producer,
    private val consumerScheduler: ConsumerScheduler,
): ShouldSpec() {
    init {
        context("Machado") {
            should("send and consume messages") {
                val topic = randomString()
                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessages,
                    topic = topic
                )
                consumerScheduler.registerConsumer(consumer)

                val message = randomString()
                producer.produceMessage(topic, message)
                delay(500)
                consumedMessages.size shouldBe 1
                consumedMessages[0].payload shouldBe message
            }
        }
    }
}