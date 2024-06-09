package lgs.publisher_consumer.sql_based

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import lgs.configuration.DatabaseFactory
import lgs.configuration.Databases
import lgs.test_helpers.cleanDbFile
import lgs.test_helpers.createTestClock
import lgs.test_helpers.randomString
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction

class SqlBasedPublisherTest(
) : ShouldSpec() {
    private val dbFile = "./dbs/sql_based_publisher.db"
    private val db by lazy {
        cleanDbFile(dbFile)
        DatabaseFactory().createDatabase(dbFile)
    }

    override fun afterSpec(f: suspend (Spec) -> Unit) {
        super.afterSpec(f)
        cleanDbFile(dbFile)
    }

    init {
        context("send") {
            should("store message") {
                val clock = createTestClock()
                val publisher = SqlBasedProducer(db = Databases(db, db), clock = clock)

                val topic = randomString()
                val payload = randomString()
                publisher.produceMessage(topic, payload)

                transaction(db) {
                    val messages = MessageTable.selectAll()
                        .where { MessageTable.topic eq topic }
                        .map { it.message() }
                    messages.size shouldBe 1
                    messages.first().payload shouldBe payload
                }
            }
        }
    }
}
