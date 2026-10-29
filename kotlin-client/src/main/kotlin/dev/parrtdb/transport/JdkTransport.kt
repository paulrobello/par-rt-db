package dev.parrtdb.transport

import dev.parrtdb.http.await
import dev.parrtdb.ws.WsConnection
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/** The default transport, backed by `java.net.http`. JVM only: the JDK HTTP client does not exist on Android. */
class JdkTransport(private val http: HttpClient = HttpClient.newHttpClient()) : StreamingHttpTransport, WsTransport {
    override suspend fun send(request: TransportRequest): TransportResponse {
        val body = request.body
        val publisher = if (body != null) HttpRequest.BodyPublishers.ofByteArray(body) else HttpRequest.BodyPublishers.noBody()
        val builder = HttpRequest.newBuilder(URI.create(request.url)).method(request.method, publisher)
        for ((k, v) in request.headers) builder.header(k, v)
        val resp = http.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofByteArray()).await()
        return TransportResponse(resp.statusCode(), resp.body())
    }

    override suspend fun sendStreaming(request: StreamingRequest): StreamingResponse {
        val body = request.body
        val publisher = if (body != null) HttpRequest.BodyPublishers.ofInputStream { body } else HttpRequest.BodyPublishers.noBody()
        val builder = HttpRequest.newBuilder(URI.create(request.url)).method(request.method, publisher)
        for ((k, v) in request.headers) builder.header(k, v)
        val resp = http.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofInputStream()).await()
        return StreamingResponse(resp.statusCode(), resp.body())
    }

    override suspend fun connect(url: String, headers: Map<String, String>): WsSession =
        WsConnection.dial(http, URI.create(url), headers)
}
