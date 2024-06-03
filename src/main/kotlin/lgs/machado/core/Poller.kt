package lgs.machado.core

import lgs.machado.Consumer
import lgs.machado.model.Message

interface Poller {
    suspend fun pollMessages(consumer: Consumer, maxPollSize: Int = 1): List<Message>
    suspend fun markMessagesAsConsumed(consumer: Consumer, messages: List<Message>)
}