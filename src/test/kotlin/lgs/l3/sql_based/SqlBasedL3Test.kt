package lgs.l3.sql_based

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import lgs.configuration.DatabaseFactory
import lgs.l3.model.Item
import lgs.test_helpers.cleanDbFile
import lgs.test_helpers.createTestClock
import lgs.test_helpers.randomByteArray
import lgs.test_helpers.randomString
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

class SqlBasedL3Test(
): ShouldSpec() {
    private val dbFile = "./sql_based_l3.db"
    private val db by lazy {
        cleanDbFile(dbFile)
        DatabaseFactory().database(dbFile)
    }
    private val clock by lazy { createTestClock() }
    private val l3 by lazy { SqlBasedL3(db = db, clock = clock) }

    override fun afterSpec(f: suspend (Spec) -> Unit) {
        super.afterSpec(f)
        cleanDbFile(dbFile)
    }

    init {
        context("putItem") {
            should("store content with version 1 if it is the first item for the key") {
                val key = randomString()
                val content = randomByteArray()
                val item = l3.putItem(key, content)
                item shouldBe Item(
                    key = key,
                    version = 1,
                    insertedAt = Instant.now(clock),
                    deletedAt = null,
                    content = content
                )
            }

            should("store content incrementing the version if it there is an existing item with the key") {
                val key = randomString()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    val content = randomByteArray()
                    val item = l3.putItem(key, content)
                    item shouldBe Item(
                        key = key,
                        version = i,
                        insertedAt = Instant.now(clock),
                        deletedAt = null,
                        content = content
                    )
                }
            }
        }

        context("getItem") {
            should("retrieve latest stored item when no version is provided") {
                val key = randomString()
                val content = randomByteArray()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(key, content)
                }

                val item = l3.getItem(key)
                item shouldBe Item(
                    key = key,
                    version = 5,
                    insertedAt = Instant.now(clock),
                    deletedAt = null,
                    content = content
                )
            }

            should("return null when key is not found") {
                val item = l3.getItem(randomString())
                item shouldBe null
            }

            should("retrieve deleted item") {
                val baseInstant = Instant.now(clock)
                val key = randomString()
                val content = randomByteArray()
                l3.putItem(key, content)

                clock.plus(10.seconds)
                l3.deleteItem(key)

                val item = l3.getItem(key)
                item shouldBe Item(
                    key = key,
                    version = 1,
                    insertedAt = baseInstant,
                    deletedAt = baseInstant.plusSeconds(10),
                    content = content
                )
            }

            should("retrieve item with specific version") {
                val baseInstant = Instant.now(clock)
                val key = randomString()
                val content = randomByteArray()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(key, content)
                }

                val item = l3.getItem(key, version = 3)
                item shouldBe Item(
                    key = key,
                    version = 3,
                    insertedAt = baseInstant.plusSeconds(10*3),
                    deletedAt = null,
                    content = content
                )
            }

            should("return null when version is not found") {
                val key = randomString()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(key, randomByteArray())
                }
                val item = l3.getItem(key, version = 8)
                item shouldBe null
            }
        }

        context("deleteItem") {
            should("mark the latest item as deleted") {
                val baseInstant = Instant.now(clock)
                val key = randomString()
                val content = randomByteArray()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(key, content)
                }
                clock.plus(10.seconds)
                val item = l3.deleteItem(key)
                item shouldBe Item(
                    key = key,
                    version = 5,
                    insertedAt = baseInstant.plusSeconds(10*5),
                    deletedAt = Instant.now(clock),
                    content = content
                )
            }

            should("return null and be no-op if item is not found") {
                val item = l3.deleteItem(randomString())
                item shouldBe null
            }
        }
    }
}
