package io.micronaut.docs.server.binding

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.RxHttpClient
import io.micronaut.http.uri.UriTemplate

class BookmarkControllerTest: StringSpec() {

    val embeddedServer = autoClose(
            ApplicationContext.run(EmbeddedServer::class.java)
    )

    val client = autoClose(
            embeddedServer.applicationContext.createBean(RxHttpClient::class.java, embeddedServer.getURL())
    )

    init {
        "test bookmark controller" {
            var template = UriTemplate("/api/bookmarks/list{?offset,max,sort,order}")
            var uri = template.expand(mapOf("offset" to 0, "max" to 10))

            var response = client.toBlocking().exchange<Any>(uri)

            response.status shouldBe HttpStatus.OK
        }
    }
}
