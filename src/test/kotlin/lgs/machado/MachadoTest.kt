package lgs.machado

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import lgs.machado.core.ConsumerScheduler
import lgs.test_helpers.createTrackingConsumer
import lgs.test_helpers.randomString
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.time.Duration
import kotlin.time.measureTime


@MicronautTest
class MachadoTest(
    private val producer: Producer,
    private val consumerScheduler: ConsumerScheduler,
) : ShouldSpec() {
    init {
        context("Machado") {
            should("send and consume messages") {
                val topic = randomString()
                val consumedMessages = mutableListOf<Message>()
                val consumer = createTrackingConsumer(
                    consumedMessagesTracker = consumedMessages,
                    topic = topic
                )
                consumerScheduler.registerConsumer(consumer)

                val message = randomString()
                producer.produceMessage(topic, message)
                delay(500)
                consumedMessages.size shouldBe 1
                consumedMessages[0].payload shouldBe message
            }
        }

        // Todo: refactor this. I made this as a quick hack to test it a bit, but it needs love
        should("test") {
            val parallelism = Runtime.getRuntime().availableProcessors()
            val dispatcher = Executors.newFixedThreadPool(parallelism)
                .asCoroutineDispatcher()
            println("number of cores: $parallelism")

            val consumedMessages = mutableListOf<Message>()
            val topic = randomString()
            val group = randomString()

            val syncLatencies = Collections.synchronizedList(ArrayList<Duration>())
            val asyncLatencies = ConcurrentHashMap<String, Pair<Instant, Instant?>>()
            val numberOfMessages = 1000
            val jobs = List(numberOfMessages) {
                async(dispatcher) {
                    try {
                        val duration = measureTime {
                            val message = randomString()
                            asyncLatencies[message] = Pair(Instant.now(), null)
                            producer.produceMessage(topic, message)
                        }
                        syncLatencies.add(duration)
                    } catch (ex: Exception) {
                        println(ex)
                    }
                }
            }
            val consumer = object : Consumer {
                override fun group() = group
                override fun topic() = topic
                override suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure> {
                    messages.forEach {
                        asyncLatencies[it.payload] = Pair(asyncLatencies[it.payload]!!.first, Instant.now())
                    }
                    consumedMessages.addAll(messages)
                    return emptyList()
                }
            }
            consumerScheduler.registerConsumer(consumer)
            asyncLatencies.values.map { it.second }
            jobs.forEach { it.join() }
            println("produceMessage p90=${percentile(syncLatencies, 90)} p99=${percentile(syncLatencies, 99)}")
            while (consumedMessages.size < numberOfMessages) {
                delay(500)
            }
            consumedMessages.size shouldBe numberOfMessages
        }
    }

    private fun percentile(latencies: List<Duration>, percentile: Int): Duration {
        val sortedLatencies = latencies
            .sorted()
        val index = kotlin.math.ceil(percentile.toDouble() / 100 * latencies.size)
        return sortedLatencies[(index - 1).toInt()]
    }
}