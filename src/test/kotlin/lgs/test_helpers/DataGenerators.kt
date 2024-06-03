package lgs.test_helpers

import java.nio.charset.Charset
import java.util.UUID

fun randomString() = UUID.randomUUID().toString()
fun randomByteArray() = randomString().toByteArray(Charset.defaultCharset())