package lgs.publisher_consumer

/**
 * The producer_consumer package has a basic Publisher / Consumer implementation.
 * Using [Producer], you are able to send string payloads to a topic for async processing.
 * A [Consumer] polls and consumes messages from the [Consumer.topic]. And, you can have multiple consumers
 * polling from the same [Consumer.topic]. But, if they share the same [Consumer.group], a [Message]
 * is going to be considered consumed when processed by any of the groups consumers.
 *
 * Key facts:
 * - [Message] consume guarantee is **at least once**.
 * - [Message] consume order is **not guaranteed**, but older messages are prioritized.
 * - [Message] consume is controlled by [Consumer.topic] and [Consumer.group]. A [Message]
 * is considered to be consumed by the group when it is consumed by any [Consumer] in the same [Consumer.group].
 * - If you introduce a new [Consumer.group], its consumers will process **the whole topic**.
 */
interface Producer {
    /**
     * @param [topic] that the payload should be posted to.
     * @param [payload] string containing the message. Can be Base64, Json, or whatever your heart desires.
     */
    suspend fun produceMessage(topic: String, payload: String)
}