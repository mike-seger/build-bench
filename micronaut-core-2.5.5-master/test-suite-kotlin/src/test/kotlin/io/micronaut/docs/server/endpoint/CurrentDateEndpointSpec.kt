package io.micronaut.docs.server.endpoint

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.client.RxHttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.Test

import java.util.Date
import java.util.HashMap

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue

class CurrentDateEndpointSpec: StringSpec() {

    val embeddedServer = autoClose(
            ApplicationContext.run(EmbeddedServer::class.java)
    )

    val client = autoClose(
            embeddedServer.applicationContext.createBean(RxHttpClient::class.java, embeddedServer.url)
    )

    init {
        "test read custom date endpoint" {
            val response = client.exchange("/date", String::class.java).blockingFirst()

            response.code() shouldBe HttpStatus.OK.code
        }

        "test read custom date endpoint with argument" {
            val response = client.exchange("/date/current_date_is", String::class.java).blockingFirst()

            response.code() shouldBe HttpStatus.OK.code
            response.body()!!.startsWith("current_date_is: ") shouldBe true
        }

        // issue https://github.com/micronaut-projects/micronaut-core/issues/883
        "test read with produces" {
            val response = client.exchange("/date/current_date_is", String::class.java).blockingFirst()

            response.contentType.get() shouldBe MediaType.TEXT_PLAIN_TYPE
        }

        "test write custom date endpoint" {
            val originalDate: Date
            val resetDate: Date

            var response = client.exchange("/date", String::class.java).blockingFirst()
            originalDate = Date(java.lang.Long.parseLong(response.body()!!))

            response = client.exchange(HttpRequest.POST<Map<String, Any>>("/date", mapOf()), String::class.java).blockingFirst()

            response.code() shouldBe HttpStatus.OK.code
            response.body() shouldBe "Current date reset"

            response = client.exchange("/date", String::class.java).blockingFirst()
            resetDate = Date(java.lang.Long.parseLong(response.body()!!))

            assert(resetDate.time > originalDate.time)
        }

        "test disable endpoint" {
            embeddedServer.stop() // top the previously created server otherwise a port conflict will occur

            val server = ApplicationContext.run(EmbeddedServer::class.java, mapOf("custom.date.enabled" to false))
            val rxClient = server.applicationContext.createBean(RxHttpClient::class.java, server.url)

            try {
                rxClient.exchange("/date", String::class.java).blockingFirst()
            } catch (ex: HttpClientResponseException) {
                ex.response.code() shouldBe HttpStatus.NOT_FOUND.code
            }

            server.close()
        }
    }
}
