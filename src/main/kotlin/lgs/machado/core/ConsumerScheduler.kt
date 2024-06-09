package lgs.machado.core

import io.micronaut.context.ApplicationContext
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import lgs.machado.Consumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

@Singleton
class ConsumerScheduler(
    private val applicationContext: ApplicationContext,
    private val consumerManager: ConsumerManager,
) {
    private val logger: Logger = LoggerFactory.getLogger(ConsumerScheduler::class.java)
    private val scope = CoroutineScope(Dispatchers.Default)
    private val consumers = mutableListOf<Consumer>()

    @PostConstruct
    fun registerConsumersOnAppContext() {
        val consumerBeans = applicationContext.getBeansOfType(Consumer::class.java)
        consumerBeans.forEach { registerConsumer(it) }
    }

    fun registerConsumer(consumer: Consumer) {
        consumers.add(consumer)
    }

    private val consumeMap = ConcurrentHashMap<String, Boolean>()

    @Scheduled(fixedRate = "PT0.2S")
    fun pollAndConsumeMessages() {
        val maxPollSize = 10
        logger.debug("Scheduling ${consumers.size} consumers with a maxPollSize $maxPollSize")
        consumers.forEach { consumer ->
            scope.launch {
                val key = "${consumer.group()}-${consumer.topic()}"
                if (consumeMap.getOrDefault(key, false)) {
                    return@launch
                }
                consumeMap[key] = true
                consumerManager.consumeMessages(consumer, maxPollSize)
                consumeMap.remove(key)
            }
        }
    }
}