package lgs.controller

import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import jakarta.inject.Singleton
import lgs.l3.L3
import lgs.machado.ConsumeFailure
import lgs.machado.Consumer
import lgs.machado.Message
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

@Controller("/compliance-artifacts")
class ComplianceArtifactController(
    private val l3: L3,
) {
    @Post(consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.APPLICATION_JSON])
    suspend fun uploadComplianceArtifact(artifact: CompletedFileUpload): HttpResponse<String> {
        val uploadedItem = l3.putItem("compliance-artifacts", UUID.randomUUID().toString(), artifact.bytes)
        return HttpResponse
            .created(uploadedItem.key)
    }
}

@Singleton
class ConsumerA : Consumer {
    private val logger: Logger = LoggerFactory.getLogger(ConsumerA::class.java)
    override fun group() = "group"
    override fun topic() = "l3-ie-compliance-artifacts"
    override suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure> {
        messages.forEach { logger.info("consumed ${it.payload}") }
        return emptyList()
    }
}
