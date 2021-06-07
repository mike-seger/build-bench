package io.micronaut.docs.annotation

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.RxHttpClient
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException

import javax.validation.ConstraintViolationException

import org.junit.Assert.assertEquals
import java.lang.Exception

class PetControllerSpec: StringSpec() {

    val embeddedServer = autoClose(
            ApplicationContext.run(EmbeddedServer::class.java)
    )

    val client = autoClose(
            embeddedServer.applicationContext.createBean(RxHttpClient::class.java, embeddedServer.url)
    )

    init {
        "test post pet" {
            val client = embeddedServer.applicationContext.getBean(PetClient::class.java)

            // tag::post[]
            val pet = client.save("Dino", 10).blockingGet()

            pet.name shouldBe "Dino"
            pet.age.toLong() shouldBe 10
            // end::post[]
        }

        "test post pet validation" {
            val client = embeddedServer.applicationContext.getBean(PetClient::class.java)

            // tag::error[]
            try {
                client.save("Fred", -1).blockingGet()
            } catch (e: Exception) {
                e.javaClass shouldBe ConstraintViolationException::class.java
                e.message shouldBe "save.age: must be greater than or equal to 1"
            }
            // end::error[]
        }
    }

    // tag::errorRule[]
    // end::errorRule[]
}
