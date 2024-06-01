package lgs

import io.micronaut.runtime.Micronaut

fun main(args: Array<String>) {
    Micronaut
        .build()
        .eagerInitSingletons(true)
        .args(*args)
        .start()
}

