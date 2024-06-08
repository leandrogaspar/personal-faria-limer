package lgs.l3

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import lgs.l3.model.ItemEvent
import lgs.machado.ConsumerScheduler
import lgs.machado.model.Message
import lgs.test_helpers.createTrackingConsumer
import lgs.test_helpers.randomByteArray
import lgs.test_helpers.randomString

@MicronautTest
class L3Test(
    private val l3: L3,
    private val consumerScheduler: ConsumerScheduler,
) : ShouldSpec() {
    init {
        context("putItem") {
            should("generate ItemEvent") {
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()

                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessages,
                    topic = "l3-ie-${folder}"
                )
                consumerScheduler.registerConsumer(consumer)

                val item = l3.putItem(folder, key, content)
                delay(500)
                consumedMessages.size shouldBe 1
                val itemEvent = Json.decodeFromString(ItemEvent.serializer(), consumedMessages[0].payload)
                itemEvent.folder shouldBe folder
                itemEvent.key shouldBe key
                itemEvent.version shouldBe 1
                itemEvent.deletedAt shouldBe null
                itemEvent.insertedAt shouldBe item.insertedAt
            }
        }

        context("deleteItem") {
            should("generate ItemEvent") {
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()

                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessages,
                    topic = "l3-ie-${folder}"
                )
                consumerScheduler.registerConsumer(consumer)

                l3.putItem(folder, key, content)
                delay(500)
                val deletedItem = l3.deleteItem(folder, key)
                delay(500)
                consumedMessages.size shouldBe 2
                val itemEvent = Json.decodeFromString(ItemEvent.serializer(), consumedMessages[1].payload)
                itemEvent.folder shouldBe folder
                itemEvent.key shouldBe key
                itemEvent.version shouldBe 1
                itemEvent.deletedAt shouldBe deletedItem?.deletedAt
                itemEvent.insertedAt shouldBe deletedItem?.insertedAt
            }
        }
    }
}