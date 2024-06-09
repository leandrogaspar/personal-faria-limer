package lgs.publisher_consumer.core

import lgs.publisher_consumer.Consumer

interface ConsumerManager {
    suspend fun consumeMessages(consumer: Consumer, maxPollSize: Int = 1)
}