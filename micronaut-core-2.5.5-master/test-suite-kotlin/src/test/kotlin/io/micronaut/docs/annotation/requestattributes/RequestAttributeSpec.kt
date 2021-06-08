package io.micronaut.docs.annotation.requestattributes

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.RxHttpClient
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.Assert
import org.junit.Test

class RequestAttributeSpec: StringSpec() {

    val embeddedServer = autoClose(
            ApplicationContext.run(EmbeddedServer::class.java)
    )

    init {
        "test sender attributes" {
            val client = embeddedServer.applicationContext.getBean(StoryClient::class.java)
            val filter = embeddedServer.applicationContext.getBean(StoryClientFilter::class.java)

            val story = client.getById("jan2019").blockingGet()
            val attributes = filter.latestRequestAttributes

            story shouldNotBe null
            attributes shouldNotBe null

            attributes.get("story-id") shouldBe "jan2019"
            attributes.get("client-name") shouldBe "storyClient"
            attributes.get("version") shouldBe "1"
        }
    }
}
