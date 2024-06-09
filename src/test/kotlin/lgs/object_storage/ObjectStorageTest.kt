package lgs.object_storage

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import lgs.publisher_consumer.Message
import lgs.publisher_consumer.core.ConsumerScheduler
import lgs.test_helpers.createTrackingConsumer
import lgs.test_helpers.randomByteArray
import lgs.test_helpers.randomString

@MicronautTest
class ObjectStorageTest(
    private val objectStorage: ObjectStorage,
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
                    topic = "obj-storage-ie-${folder}"
                )
                consumerScheduler.registerConsumer(consumer)

                val item = objectStorage.putItem(folder, key, content)
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
                    topic = "obj-storage-ie-${folder}"
                )
                consumerScheduler.registerConsumer(consumer)

                objectStorage.putItem(folder, key, content)
                delay(500)
                val deletedItem = objectStorage.deleteItem(folder, key)
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