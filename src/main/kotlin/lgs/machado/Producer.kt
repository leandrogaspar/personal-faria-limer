package lgs.machado

interface Producer {
    suspend fun produceMessage(topic: String, payload: String)
}