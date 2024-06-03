package lgs.machado.core

import io.micronaut.context.ApplicationContext
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import kotlinx.coroutines.*
import lgs.machado.Consumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory


@Singleton
class Machado(
    private val applicationContext: ApplicationContext,
    private val poller: Poller,
) {
    private val logger: Logger = LoggerFactory.getLogger(Machado::class.java)
    private val scope = CoroutineScope(Dispatchers.Default)
    private val consumers: Collection<Consumer> by lazy {
        applicationContext.getBeansOfType(Consumer::class.java)
    }

    @Scheduled(fixedRate = "PT3S")
    fun pollAndConsumeMessages() {
        consumers.forEach { consumer ->
            scope.launch {
                val messages = poller.pollMessages(consumer, 10)
                logger.info("Got ${messages.size} messages on topic ${consumer.consumerTopic()} for group ${consumer.consumerGroup()}")
                messages.forEach { message ->
                    consumer.consumeMessage(message)
                    poller.markMessagesAsConsumed(consumer, listOf(message))
                }
            }
        }
    }
}