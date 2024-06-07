package lgs.machado.core

import lgs.machado.Consumer

interface ConsumerManager {
    suspend fun consumeMessages(consumer: Consumer, maxPollSize: Int = 1)
}