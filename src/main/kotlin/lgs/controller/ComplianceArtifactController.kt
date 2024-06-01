package lgs.controller

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import lgs.l3.L3
import lgs.l3.impl.InDatabaseL3
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

@Controller("/compliance-artifacts")
class ComplianceArtifactController(
    private val l3: L3
) {
    private val logger: Logger = LoggerFactory.getLogger(ComplianceArtifactController::class.java)
    @Post(consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.APPLICATION_JSON])
    suspend fun uploadComplianceArtifact(artifact: CompletedFileUpload, request: HttpRequest<*>?): HttpResponse<Any> {
        l3.putItem(UUID.randomUUID().toString(), artifact.bytes)
        return HttpResponse
            .created("sifafofas")
    }
}