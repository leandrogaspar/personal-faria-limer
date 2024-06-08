package lgs.l3.sql_based

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import lgs.configuration.DatabaseFactory
import lgs.configuration.Databases
import lgs.l3.model.Item
import lgs.machado.Producer
import lgs.test_helpers.cleanDbFile
import lgs.test_helpers.createTestClock
import lgs.test_helpers.randomByteArray
import lgs.test_helpers.randomString
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

class SqlBasedL3Test(
) : ShouldSpec() {
    private val dbFile = "./dbs/sql_based_l3.db"
    private val db by lazy {
        cleanDbFile(dbFile)
        DatabaseFactory().createDatabase(dbFile)
    }
    private val clock by lazy { createTestClock() }
    private val producer = object : Producer {
        override suspend fun produceMessage(topic: String, payload: String) {
        }
    }
    private val l3 by lazy { SqlBasedL3(db = Databases(db, db), clock = clock, producer = producer) }

    override fun afterSpec(f: suspend (Spec) -> Unit) {
        super.afterSpec(f)
        cleanDbFile(dbFile)
    }

    init {
        context("putItem") {
            should("store content with version 1 if it is the first item for the key") {
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()
                val item = l3.putItem(folder, key, content)
                item shouldBe Item(
                    folder = folder,
                    key = key,
                    version = 1,
                    insertedAt = Instant.now(clock),
                    deletedAt = null,
                    content = content
                )
            }

            should("store content incrementing the version if it there is an existing item with the key") {
                val folder = randomString()
                val key = randomString()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    val content = randomByteArray()
                    val item = l3.putItem(folder, key, content)
                    item shouldBe Item(
                        folder = folder,
                        key = key,
                        version = i,
                        insertedAt = Instant.now(clock),
                        deletedAt = null,
                        content = content
                    )
                }
            }

            should("store items with same keys but different folders independently") {
                val folderA = randomString()
                val folderB = randomString()
                val key = randomString()
                val content = randomByteArray()

                val baseInstant = Instant.now(clock)
                val itemA = l3.putItem(folderA, key, content)
                clock.plus(10.seconds)
                val itemB = l3.putItem(folderB, key, content)
                itemA shouldBe Item(
                    folder = folderA,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant,
                    deletedAt = null,
                    content = content
                )
                itemB shouldBe Item(
                    folder = folderB,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant.plusSeconds(10),
                    deletedAt = null,
                    content = content
                )
            }
        }

        context("getItem") {
            should("retrieve latest stored item when no version is provided") {
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(folder, key, content)
                }

                val item = l3.getItem(folder, key)
                item shouldBe Item(
                    folder = folder,
                    key = key,
                    version = 5,
                    insertedAt = Instant.now(clock),
                    deletedAt = null,
                    content = content
                )
            }

            should("retrieve items with same keys but different folders independently") {
                val folderA = randomString()
                val folderB = randomString()
                val key = randomString()
                val content = randomByteArray()

                val baseInstant = Instant.now(clock)
                l3.putItem(folderA, key, content)
                clock.plus(10.seconds)
                l3.putItem(folderB, key, content)
                val itemA = l3.getItem(folderA, key)
                val itemB = l3.getItem(folderB, key)
                itemA shouldBe Item(
                    folder = folderA,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant,
                    deletedAt = null,
                    content = content
                )
                itemB shouldBe Item(
                    folder = folderB,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant.plusSeconds(10),
                    deletedAt = null,
                    content = content
                )
            }

            should("return null when key is not found") {
                val item = l3.getItem(randomString(), randomString())
                item shouldBe null
            }

            should("retrieve deleted item") {
                val baseInstant = Instant.now(clock)
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()
                l3.putItem(folder, key, content)

                clock.plus(10.seconds)
                l3.deleteItem(folder, key)

                val item = l3.getItem(folder, key)
                item shouldBe Item(
                    folder = folder,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant,
                    deletedAt = baseInstant.plusSeconds(10),
                    content = content
                )
            }

            should("retrieve item with specific version") {
                val baseInstant = Instant.now(clock)
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(folder, key, content)
                }

                val item = l3.getItem(folder, key, version = 3)
                item shouldBe Item(
                    folder = folder,
                    key = key,
                    version = 3,
                    insertedAt = baseInstant.plusSeconds(10 * 3),
                    deletedAt = null,
                    content = content
                )
            }

            should("return null when version is not found") {
                val folder = randomString()
                val key = randomString()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(folder, key, randomByteArray())
                }
                val item = l3.getItem(folder, key, version = 8)
                item shouldBe null
            }
        }

        context("deleteItem") {
            should("mark the latest item as deleted") {
                val baseInstant = Instant.now(clock)
                val folder = randomString()
                val key = randomString()
                val content = randomByteArray()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(folder, key, content)
                }
                clock.plus(10.seconds)
                val item = l3.deleteItem(folder, key)
                item shouldBe Item(
                    folder = folder,
                    key = key,
                    version = 5,
                    insertedAt = baseInstant.plusSeconds(10 * 5),
                    deletedAt = Instant.now(clock),
                    content = content
                )
            }

            should("delete items with same keys but different folders independently") {
                val folderA = randomString()
                val folderB = randomString()
                val key = randomString()
                val content = randomByteArray()

                val baseInstant = Instant.now(clock)
                l3.putItem(folderA, key, content)
                clock.plus(10.seconds)
                l3.putItem(folderB, key, content)
                clock.plus(10.seconds)
                val deletedItemA = l3.deleteItem(folderA, key)
                val itemB = l3.getItem(folderB, key)
                deletedItemA shouldBe Item(
                    folder = folderA,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant,
                    deletedAt = baseInstant.plusSeconds(20),
                    content = content
                )
                itemB shouldBe Item(
                    folder = folderB,
                    key = key,
                    version = 1,
                    insertedAt = baseInstant.plusSeconds(10),
                    deletedAt = null,
                    content = content
                )
            }

            should("return null and be no-op if item is not found") {
                val item = l3.deleteItem(randomString(), randomString())
                item shouldBe null
            }
        }
    }
}
