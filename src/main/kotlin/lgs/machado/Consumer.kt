package lgs.machado

import lgs.machado.model.Message

interface Consumer {
    fun consumerGroup(): String
    fun consumerTopic(): String
    suspend fun consumeMessage(message: Message)
}
