package dev.parrtdb.net

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.CopyOnWriteArrayList

class Recorded(val method: String, val path: String, val query: String?, val headers: Map<String, List<String>>, val body: String) {
    fun header(name: String): String? = headers.entries.firstOrNull { it.key.equals(name, true) }?.value?.firstOrNull()
}

/** JDK HttpServer that records requests and answers with a scripted (status, body, contentType) per call. */
class TestHttpServer : AutoCloseable {
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    val requests = CopyOnWriteArrayList<Recorded>()
    var respond: (Recorded) -> Triple<Int, String, String> = { Triple(200, "{}", "application/json") }

    init {
        server.createContext("/") { ex ->
            val body = ex.requestBody.readBytes().toString(Charsets.UTF_8)
            val rec = Recorded(ex.requestMethod, ex.requestURI.rawPath, ex.requestURI.rawQuery, ex.requestHeaders.toMap(), body)
            requests += rec
            val (status, out, ct) = respond(rec)
            val bytes = out.toByteArray()
            ex.responseHeaders.add("Content-Type", ct)
            ex.sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
            if (bytes.isNotEmpty()) ex.responseBody.use { it.write(bytes) } else ex.close()
        }
        server.start()
    }

    val baseUrl: String get() = "http://127.0.0.1:${server.address.port}"

    fun reply(status: Int = 200, body: String = "{}") {
        respond = { Triple(status, body, "application/json") }
    }

    override fun close() = server.stop(0)
}
