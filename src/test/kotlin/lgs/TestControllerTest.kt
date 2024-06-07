package lgs;

import io.kotest.core.spec.style.FunSpec
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.kotest5.annotation.MicronautTest

@MicronautTest
class TestControllerTest(
    @Client("/") private val client: HttpClient,
) : FunSpec({
    test("testing L3") {
        val content = "this is a test"
    }
})
