package lgs.controller

import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import jakarta.inject.Singleton
import lgs.l3.L3
import lgs.machado.Consumer
import lgs.machado.ConsumeFailure
import lgs.machado.Producer
import lgs.machado.model.Message
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

@Controller("/compliance-artifacts")
class ComplianceArtifactController(
    private val l3: L3,
    private val producer: Producer
) {
    private val logger: Logger = LoggerFactory.getLogger(ComplianceArtifactController::class.java)
    private var counter: Int = 0

    @Post(consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.APPLICATION_JSON])
    suspend fun uploadComplianceArtifact(artifact: CompletedFileUpload): HttpResponse<String> {
        val uploadedItem = l3.putItem(UUID.randomUUID().toString(), artifact.bytes)
        val message = "payload ${counter++}"
        logger.info("Publishing message with payload: $message")
        producer.produceMessage("test", message)
        return HttpResponse
            .created("sifafofas")
    }
}
