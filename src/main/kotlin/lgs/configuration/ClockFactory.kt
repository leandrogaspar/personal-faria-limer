package lgs.configuration

import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.time.Clock

@Factory
class ClockFactory {
    @Singleton
    fun clock() = Clock.systemUTC()
}