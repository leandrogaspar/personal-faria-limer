package lgs.machado

interface Producer {
    suspend fun send(topic: String, payload: String)
}