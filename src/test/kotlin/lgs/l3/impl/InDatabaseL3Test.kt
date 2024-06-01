package lgs.l3.impl

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.extensions.clock.TestClock
import io.kotest.matchers.shouldBe
import lgs.configuration.DatabaseFactory
import lgs.l3.Item
import java.io.File
import java.nio.charset.Charset
import java.time.Instant
import java.time.ZoneOffset
import java.util.*
import kotlin.time.Duration.Companion.seconds

class InDatabaseL3Test(
): ShouldSpec() {
    private val dbFile = "./in_database_l3_test.db"
    private val db by lazy {
        cleanDbFile()
        DatabaseFactory().database(dbFile)
    }

    override fun afterSpec(f: suspend (Spec) -> Unit) {
        super.afterSpec(f)
        cleanDbFile()
    }

    init {
        context("putItem") {
            should("store content with version 1 if it is the first item for the key") {
                val clock = createTestClock()
                val l3 = InDatabaseL3(db = db, clock = clock)
                val key = mockKey()
                val content = mockContent()
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
                val clock = createTestClock()
                val l3 = InDatabaseL3(db = db, clock = clock)
                val key = mockKey()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    val content = mockContent()
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
                val clock = createTestClock()
                val l3 = InDatabaseL3(db = db, clock = clock)

                val key = mockKey()
                val content = mockContent()
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
                val clock = createTestClock()
                val l3 = InDatabaseL3(db = db, clock = clock)
                val item = l3.getItem(mockKey())
                item shouldBe null
            }

            should("retrieve deleted item") {
                val clock = createTestClock()
                val baseInstant = Instant.now(clock)
                val l3 = InDatabaseL3(db = db, clock = clock)

                val key = mockKey()
                val content = mockContent()
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
                val clock = createTestClock()
                val baseInstant = Instant.now(clock)
                val l3 = InDatabaseL3(db = db, clock = clock)

                val key = mockKey()
                val content = mockContent()
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
                val clock = createTestClock()
                val l3 = InDatabaseL3(db = db, clock = clock)
                val key = mockKey()
                for (i in (1..5)) {
                    clock.plus(10.seconds)
                    l3.putItem(key, mockContent())
                }
                val item = l3.getItem(key, version = 8)
                item shouldBe null
            }
        }

        context("deleteItem") {
            should("mark the latest item as deleted") {
                val clock = createTestClock()
                val baseInstant = Instant.now(clock)
                val l3 = InDatabaseL3(db = db, clock = clock)
                val key = mockKey()
                val content = mockContent()
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
                val clock = createTestClock()
                val l3 = InDatabaseL3(db = db, clock = clock)
                val item = l3.deleteItem(mockKey())
                item shouldBe null
            }
        }
    }

    // We only store epochMilli in DB, so our Instant read from DB lacks the nano part. When comparing against our
    // in memory Instant.now() the assertion would fail. Therefore, we truncate to milliSeconds.
    // Todo: clean the Instant.ofEpochMilli(Instant.now().toEpochMilli()) hack - for some reason IntelliJ is
    //   not picking ChronoUnit so I can't use Instant.now().truncatedTo(ChronoUnit.MILLIS) lol
    private fun createTestClock() = TestClock(Instant.ofEpochMilli(Instant.now().toEpochMilli()), ZoneOffset.UTC)
    private fun mockKey() = UUID.randomUUID().toString()
    private fun mockContent() = UUID.randomUUID().toString().toByteArray(Charset.defaultCharset())
    private fun cleanDbFile() = File(dbFile).delete()
}
