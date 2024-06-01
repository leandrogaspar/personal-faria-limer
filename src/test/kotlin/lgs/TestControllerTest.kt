package lgs;

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest
import lgs.l3.impl.InDatabaseL3
import java.nio.charset.Charset
import java.util.UUID

@MicronautTest
class TestControllerTest(
    @Client("/") private val client: HttpClient,
): FunSpec({
    test("testing L3") {
        val content = "this is a test"
    }
})
