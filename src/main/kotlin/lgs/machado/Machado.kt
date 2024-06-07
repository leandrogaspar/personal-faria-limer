package lgs.machado

import io.micronaut.context.ApplicationContext
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import lgs.machado.core.ConsumerManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Singleton
class Machado(
    private val applicationContext: ApplicationContext,
    private val consumerManager: ConsumerManager,
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
                consumerManager.consumeMessages(consumer)
            }
        }
    }
}