package lgs.test_helpers

import lgs.publisher_consumer.ConsumeFailure
import lgs.publisher_consumer.Consumer
import lgs.publisher_consumer.Message

/**
 * @return a consumer that adds all received messages to the [consumedMessagesTracker] list
 */
fun createTrackingConsumer(
    consumedMessagesTracker: MutableList<Message>,
    group: String = randomString(),
    topic: String = randomString(),
): Consumer {
    return object : Consumer(group, topic) {
        override suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure> {
            consumedMessagesTracker.addAll(messages)
            return emptyList()
        }
    }
}