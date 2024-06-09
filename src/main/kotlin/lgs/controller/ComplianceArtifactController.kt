package lgs.controller

import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import jakarta.inject.Singleton
import lgs.object_storage.ObjectStorage
import lgs.publisher_consumer.ConsumeFailure
import lgs.publisher_consumer.Consumer
import lgs.publisher_consumer.Message
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

@Controller("/compliance-artifacts")
class ComplianceArtifactController(
    private val objectStorage: ObjectStorage,
) {
    @Post(consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.APPLICATION_JSON])
    suspend fun uploadComplianceArtifact(artifact: CompletedFileUpload): HttpResponse<String> {
        val uploadedItem = objectStorage.putItem("compliance-artifacts", UUID.randomUUID().toString(), artifact.bytes)
        return HttpResponse
            .created(uploadedItem.key)
    }
}

@Singleton
class ConsumerA : Consumer("group", "obj-storage-ie-compliance-artifacts") {
    private val logger: Logger = LoggerFactory.getLogger(ConsumerA::class.java)
    override suspend fun consumeMessages(messages: List<Message>): List<ConsumeFailure> {
        messages.forEach { logger.info("consumed ${it.payload}") }
        return emptyList()
    }
}
