package lgs.test_helpers

import lgs.machado.ConsumeFailure
import lgs.machado.Consumer
import lgs.machado.model.Message

/**
 * @return a consumer that adds all received messages to the [consumedMessagesTracker] list
 */
fun createTrackingConsumer(
    consumedMessagesTracker: MutableList<Message>,
    group: String = randomString(),
    topic: String = randomString(),
): Consumer {
    return object : Consumer {
        override fun group() = group
        override fun topic() = topic
        override suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure> {
            consumedMessagesTracker.addAll(messages)
            return emptyList()
        }
    }
}