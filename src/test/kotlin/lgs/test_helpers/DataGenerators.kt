package lgs.test_helpers

import java.nio.charset.Charset
import java.util.*

fun randomString() = UUID.randomUUID().toString()
fun randomByteArray() = randomString().toByteArray(Charset.defaultCharset())