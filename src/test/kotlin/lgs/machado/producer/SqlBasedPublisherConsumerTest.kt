package lgs.machado.producer

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.ShouldSpec
import lgs.configuration.DatabaseFactory
import lgs.machado.sql_based.SqlBasedPublisher
import lgs.test_helpers.cleanDbFile
import lgs.test_helpers.createTestClock
import lgs.test_helpers.randomString

class SqlBasedPublisherConsumerTest(
): ShouldSpec() {
    private val dbFile = "./sql_based_publisher.db"
    private val db by lazy {
        cleanDbFile(dbFile)
        DatabaseFactory().database(dbFile)
    }

    override fun afterSpec(f: suspend (Spec) -> Unit) {
        super.afterSpec(f)
        cleanDbFile(dbFile)
    }

    init {
        context("send") {
            should("store message") {
                val clock = createTestClock()
                val publisher = SqlBasedPublisher(db = db, clock = clock)

                val topic = randomString()
                val payload = randomString()
                publisher.send(topic, payload)
            }
        }
    }
}
