package dev.parrtdb.net

import dev.parrtdb.admin.AdminClient
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.http.RtDbHttpClient
import dev.parrtdb.transport.HttpTransport
import dev.parrtdb.transport.JdkTransport
import dev.parrtdb.transport.OkHttpTransport
import dev.parrtdb.transport.TransportRequest
import dev.parrtdb.transport.TransportResponse
import dev.parrtdb.wire.Query
import dev.parrtdb.ws.RtDbWsClient
import dev.parrtdb.ws.WsOptions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

private fun <T> run10(block: suspend kotlinx.coroutines.CoroutineScope.() -> T): T =
    runBlocking(kotlinx.coroutines.Dispatchers.Default) { withTimeout(10_000) { block() } }

class TransportTest {
    private val transports: List<Pair<String, HttpTransport>> = listOf("jdk" to JdkTransport(), "okhttp" to OkHttpTransport())

    @Test
    fun `http request shaping is identical across transports`() = run10 {
        for ((name, t) in transports) {
            TestHttpServer().use { srv ->
                srv.reply(200, """{"result":[1]}""")
                RtDbHttpClient(srv.baseUrl, "app", "tok", t).query(Query(table = "items"))
                val req = srv.requests.single()
                assertEquals("POST", req.method, name)
                assertEquals("/api/query", req.path, name)
                assertEquals("Bearer tok", req.header("Authorization"), name)
                assertEquals("3", req.header("X-Rtdb-Protocol"), name)
                assertEquals("application/json", req.header("Content-Type"), name)
            }
        }
    }

    @Test
    fun `get has no body and error envelopes map the same on every transport`() = run10 {
        for ((name, t) in transports) {
            TestHttpServer().use { srv ->
                srv.reply(404, """{"code":"NOT_FOUND","message":"gone"}""")
                val e = assertFailsWith<RtDbException>(name) { RtDbHttpClient(srv.baseUrl, "app", "t", t).download("x") }
                assertEquals(ErrorCode.NOT_FOUND, e.code, name)
                val req = srv.requests.single()
                assertEquals("GET", req.method, name)
                assertEquals("", req.body, name)
            }
        }
    }

    @Test
    fun `admin client accepts a custom transport`() = run10 {
        val seen = mutableListOf<TransportRequest>()
        val fake = object : HttpTransport {
            override suspend fun send(request: TransportRequest): TransportResponse {
                seen += request
                return TransportResponse(200, "{}".toByteArray())
            }
        }
        AdminClient("http://h:1/", "adm", fake).downloadBackup("b1")
        assertEquals("http://h:1/admin/backups/b1", seen.single().url)
        assertEquals("Bearer adm", seen.single().headers["Authorization"])
    }

    @Test
    fun `websocket client runs over okhttp`() = run10 {
        FakeWsServer().use { srv ->
            val c = RtDbWsClient(srv.url, "app", { "tok" }, WsOptions(heartbeatMs = 600_000), OkHttpTransport())
            val d = async { c.connect() }
            assertEquals("auth", (dev.parrtdb.wire.WireJson.parse(srv.next()) as kotlinx.serialization.json.JsonObject)["type"].toString().trim('"'))
            srv.send("""{"type":"authOk","user":{"kind":"machine","email":null,"name":null},"protocolVersion":3}""")
            d.await()
            c.close()
        }
    }
}
