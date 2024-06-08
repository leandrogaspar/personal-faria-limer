package lgs.test_helpers

import io.kotest.extensions.clock.TestClock
import java.time.Instant
import java.time.ZoneOffset

// We only store epochMilli in DB, so our Instant read from DB lacks the nano part. When comparing against our
// in memory Instant.now() the assertion would fail. Therefore, we truncate to milliSeconds.
// Todo: clean the Instant.ofEpochMilli(Instant.now().toEpochMilli()) hack - for some reason IntelliJ is
//   not picking ChronoUnit so I can't use Instant.now().truncatedTo(ChronoUnit.MILLIS) lol
fun createTestClock() = TestClock(Instant.ofEpochMilli(Instant.now().toEpochMilli()), ZoneOffset.UTC)