package dev.parrtdb.net

import dev.parrtdb.admin.AdminClient
import dev.parrtdb.admin.AuditQuery
import dev.parrtdb.admin.CreateWebhookOptions
import dev.parrtdb.admin.Directive
import dev.parrtdb.admin.HotConfigPatch
import dev.parrtdb.admin.MintTokenOptions
import dev.parrtdb.admin.StorageSignedUrls
import dev.parrtdb.admin.TableFilter
import dev.parrtdb.admin.WebhookEditOptions
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import kotlinx.serialization.json.jsonObject
import dev.parrtdb.http.HostCapability
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.transport.HttpTransport
import dev.parrtdb.transport.JdkTransport
import dev.parrtdb.transport.OkHttpTransport
import dev.parrtdb.transport.TransportRequest
import dev.parrtdb.transport.TransportResponse
import java.io.ByteArrayInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive

private fun <T> run10(block: suspend () -> T): T = runBlocking { withTimeout(10_000) { block() } }

class AdminClientTest {
    private fun body(r: Recorded) = WireJson.parse(r.body) as JsonObject

    @Test
    fun `create delete db and ack handling`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "adminkey")
            srv.reply(200, """{"ok":true,"creationId":"c1"}""")
            assertEquals("c1", a.createDb("app"))
            assertEquals("/admin/create-db", srv.requests.last().path)
            assertEquals("Bearer adminkey", srv.requests.last().header("Authorization"))
            assertEquals("3", srv.requests.last().header("X-Rtdb-Protocol"))
            a.deleteDb("app", "app")
            assertEquals("app", body(srv.requests.last())["confirm"]!!.jsonPrimitive.content)
            assertEquals(false, body(srv.requests.last()).containsKey("expectedCreationId"))
            a.deleteDb("app", "app", "c1")
            assertEquals("c1", body(srv.requests.last())["expectedCreationId"]!!.jsonPrimitive.content)
            srv.reply(200, """{"ok":false,"creationId":"c1"}""")
            assertEquals(ErrorCode.INTERNAL, assertFailsWith<RtDbException> { a.createDb("x") }.code)
            srv.reply(409, """{"code":"CONFLICT","message":"confirm mismatch"}""")
            assertEquals(ErrorCode.CONFLICT, assertFailsWith<RtDbException> { a.deleteDb("a", "b") }.code)
        }
    }

    @Test
    fun `admin read only toggle and conflict mapping`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"ok":true}""")
            a.setReadOnly("app", true)
            assertEquals("PATCH", srv.requests.last().method)
            assertEquals("/admin/db/app/readonly", srv.requests.last().path)
            assertEquals("true", body(srv.requests.last())["readOnly"]!!.jsonPrimitive.content)
            srv.reply(200, """{"readOnly":true}""")
            assertTrue(a.getReadOnly("app"))
            srv.reply(409, """{"code":"READ_ONLY","message":"frozen"}""")
            assertEquals(ErrorCode.READ_ONLY, assertFailsWith<RtDbException> { a.adminMutate("app", dev.parrtdb.wire.Transaction(emptyList())) }.code)
        }
    }

    @Test
    fun `mint token options and list tokens with host capability`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"tokenId":"t1","token":"secret"}""")
            a.mintToken("app", "ci")
            var b = body(srv.requests.last())
            assertEquals(setOf("db", "name"), b.keys)
            val tok = a.mintToken("app", "ci", MintTokenOptions(expiresAt = 5, readOnly = true, tables = listOf("a"), hostCapability = HostCapability.parMeshV1("macdaddy")))
            assertEquals("secret", tok.token)
            b = body(srv.requests.last())
            assertEquals("macdaddy", ((b["hostCapability"]) as JsonObject)["host"]!!.jsonPrimitive.content)
            assertEquals("5", b["expiresAt"]!!.jsonPrimitive.content)

            srv.reply(200, """{"tokens":[{"id":"t","name":"n","createdAt":1,"revoked":false,"expiresAt":null,"readOnly":true,"tables":["a"],"hostCapability":{"type":"parMeshV1","host":"h1"}},{"id":"u","name":"m","createdAt":2,"revoked":true,"expiresAt":9,"readOnly":false,"tables":[]}]}""")
            val ts = a.listTokens("app")
            assertEquals("h1", ts[0].hostCapability!!.host)
            assertNull(ts[1].hostCapability)
            assertEquals("db=app", srv.requests.last().query)
        }
    }

    @Test
    fun `response dto rejects unknown fields`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"tables":[],"totalSizeBytes":0,"bogus":1}""")
            assertFailsWith<dev.parrtdb.wire.WireException> { a.dbStats("app") }
            srv.reply(200, """{"tables":[{"name":"t","rowCount":1,"sizeBytes":2}],"totalSizeBytes":2}""")
            val s = a.dbStats("app")
            assertEquals(1, s.tables.size)
            assertEquals(0, s.subsQuota)
        }
    }

    @Test
    fun `migrate request body and result decode`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"applied":false,"schema":{"tables":{}},"directives":[{"op":"dropField","affectedRows":2,"castFailures":[{"id":"x","value":null}]}]}""")
            val r = a.migrateSchema("app", listOf(Directive.DropField("t", "f"), Directive.RenameTable("a", "b")), dryRun = true)
            assertEquals("/admin/db/app/migrate", srv.requests.last().path)
            val b = body(srv.requests.last())
            assertEquals("true", b["dryRun"]!!.jsonPrimitive.content)
            assertEquals("""[{"op":"dropField","table":"t","field":"f"},{"op":"renameTable","from":"a","to":"b"}]""", b["directives"].toString())
            assertEquals(2, r.directives[0].affectedRows)
            assertEquals(JsonNull, r.directives[0].castFailures!![0].value)
        }
    }

    @Test
    fun `json schema export route decodes`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"${'$'}schema":"https://json-schema.org/draft/2020-12/schema","${'$'}defs":{"t":{}}}""")
            val r = a.getJsonSchema("app")
            assertEquals("/admin/db/app/schema/json-schema", srv.requests.last().path)
            assertEquals(true, r.jsonObject.containsKey("${'$'}defs"))
        }
    }

    @Test
    fun `query string endpoints encode and sort params`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"entries":[]}""")
            a.getAudit("app", AuditQuery(table = "t", op = "insert", limit = 5))
            assertEquals("db=app&limit=5&op=insert&table=t", srv.requests.last().query)
            a.getAudit("app")
            assertEquals("db=app", srv.requests.last().query)
            a.schemaHistory("app", 10, 20)
            assertEquals("limit=10&offset=20", srv.requests.last().query)
            srv.reply(200, """{"ops":[]}""")
            a.opsRecent("a b", "t&x", 3)
            assertEquals("db=a+b&n=3&table=t%26x", srv.requests.last().query)
            srv.reply(200, """{"ok":true}""")
            a.cloneDb("src", "dst")
            assertEquals("POST", srv.requests.last().method)
            assertEquals("from=src&to=dst", srv.requests.last().query)
        }
    }

    @Test
    fun `webhooks create edit tri-state table filter`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"id":7}""")
            assertEquals(7, a.createWebhook("app", CreateWebhookOptions("http://h/x", events = listOf("*"))))
            assertEquals("""{"url":"http://h/x","events":["*"]}""", srv.requests.last().body)
            srv.reply(200, """{"id":7,"db":"app","table":null,"url":"u","events":["*"],"createdAt":1,"enabled":true,"secret":null}""")
            a.editWebhook("app", 7, WebhookEditOptions(table = TableFilter.Clear, enabled = false))
            assertEquals("PUT", srv.requests.last().method)
            assertEquals("""{"table":null,"enabled":false}""", srv.requests.last().body)
            a.editWebhook("app", 7, WebhookEditOptions(table = TableFilter.Scope("t")))
            assertEquals("""{"table":"t"}""", srv.requests.last().body)
            a.editWebhook("app", 7, WebhookEditOptions(rotateSecret = true))
            assertEquals("""{"rotateSecret":true}""", srv.requests.last().body)
        }
    }

    @Test
    fun `streaming export import round trips a snapshot over both transports`() = run10 {
        val snapshot = (0 until 20_000).joinToString("") { """{"i":$it,"pad":"xxxxxxxxxxxxxxxxxxxxxxxx"}""" + "\n" }
        for ((name, transport) in listOf("jdk" to JdkTransport(), "okhttp" to OkHttpTransport())) {
            TestHttpServer().use { srv ->
                val a = AdminClient(srv.baseUrl, "k", transport)
                srv.respond = { Triple(200, snapshot, "application/x-ndjson") }
                val streamed = a.exportDbStream("app").use { String(it.readAllBytes(), Charsets.UTF_8) }
                assertEquals(snapshot, streamed, name)
                assertEquals(streamed, a.exportDb("app"), name)
                assertEquals("GET", srv.requests.first().method)
                assertEquals("/admin/export-db", srv.requests.first().path)
                assertEquals("db=app", srv.requests.first().query)
                assertEquals("Bearer k", srv.requests.first().header("Authorization"))

                srv.reply(200, """{"ok":true}""")
                a.importDbStream("app", ByteArrayInputStream(snapshot.toByteArray(Charsets.UTF_8)))
                val rec = srv.requests.last()
                assertEquals("POST", rec.method)
                assertEquals("/admin/import-db", rec.path)
                assertEquals("application/x-ndjson", rec.header("Content-Type"), name)
                assertEquals(snapshot, rec.body, name)
                a.importDb("app", snapshot)
                assertEquals(snapshot, srv.requests.last().body, name)
            }
        }
    }

    @Test
    fun `streaming export import map error envelopes and ok false`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(404, """{"code":"NOT_FOUND","message":"no such db"}""")
            assertEquals(ErrorCode.NOT_FOUND, assertFailsWith<RtDbException> { a.exportDbStream("nope") }.code)
            srv.reply(409, """{"code":"CONFLICT","message":"not empty"}""")
            assertEquals(ErrorCode.CONFLICT, assertFailsWith<RtDbException> { a.importDbStream("app", ByteArrayInputStream(ByteArray(3))) }.code)
            srv.reply(200, """{"ok":false}""")
            assertEquals(ErrorCode.INTERNAL, assertFailsWith<RtDbException> { a.importDbStream("app", ByteArrayInputStream(ByteArray(3))) }.code)
        }
    }

    @Test
    fun `streaming admin calls reject a non streaming transport`() = run10 {
        val plain = object : HttpTransport {
            override suspend fun send(request: TransportRequest) = TransportResponse(200, ByteArray(0))
        }
        val a = AdminClient("http://127.0.0.1:1", "k", plain)
        assertFailsWith<UnsupportedOperationException> { a.exportDbStream("app") }
    }

    @Test
    fun `raw body routes export import upload backups`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.respond = { Triple(200, "line1\nline2\n", "application/x-ndjson") }
            assertEquals("line1\nline2\n", a.exportDb("app"))
            srv.reply(200, """{"ok":true}""")
            a.importDb("app", "{\"a\":1}\n")
            assertEquals("application/x-ndjson", srv.requests.last().header("Content-Type"))
            assertEquals("{\"a\":1}\n", srv.requests.last().body)
            srv.reply(200, """{"id":"blob1"}""")
            assertEquals("blob1", a.uploadFile("app", byteArrayOf(9), "image/png"))
            assertEquals("image/png", srv.requests.last().header("Content-Type"))
            assertEquals("/admin/db/app/storage", srv.requests.last().path)
            srv.reply(200, """{"target":"rtdb_restored_1","instructions":"go"}""")
            assertEquals("rtdb_restored_1", a.restoreBackup("dump.sql").target)
            assertEquals("""{"name":"dump.sql","confirm":"dump.sql"}""", srv.requests.last().body)
        }
    }

    @Test
    fun `sessions config and merge routes`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"ok":true,"revoked":3}""")
            assertEquals(3, a.revokeExpiredSessions().revoked)
            assertEquals("DELETE", srv.requests.last().method)
            assertEquals("expired=true", srv.requests.last().query)
            a.revokeUserSessions("u1")
            assertEquals("user=u1", srv.requests.last().query)
            srv.reply(200, """{"dbs":{"app":{"tables":{"t":2},"conflicts":[{"table":"t","id":"i"}]}},"storageRepointed":1,"sessionsRepointed":2,"anonDeleted":true}""")
            val m = a.mergeUsers("anon", "real")
            assertEquals(2, m.dbs["app"]!!.tables["t"])
            assertEquals("real", body(srv.requests.last())["confirm"]!!.jsonPrimitive.content)
            srv.reply(200, """{"port":1,"storageSignedUrls":{"mode":"databases","databases":["app"]},"hot":{"allowedOrigins":["x"],"sessionTtlDays":7},"version":"v","gitCommit":"c","admins":[]}""")
            val cfg = a.patchConfig(HotConfigPatch(perDbRpm = 100))
            assertEquals("PATCH", srv.requests.last().method)
            assertEquals("""{"perDbRpm":100}""", srv.requests.last().body)
            assertEquals(7, cfg.hot.sessionTtlDays)
            assertEquals(StorageSignedUrls("databases", listOf("app")), cfg.storageSignedUrls)
        }
    }

    @Test
    fun `enrollment grants`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"grantId":"g","grant":"secret","db":"app","capability":{"type":"parMeshV1","host":"h"},"expiresAt":9}""")
            val g = a.createEnrollmentGrant("app", "n", HostCapability.parMeshV1("h"), 9)
            assertEquals("secret", g.grant)
            assertEquals("""{"db":"app","name":"n","capability":{"type":"parMeshV1","host":"h"},"expiresAt":9}""", srv.requests.last().body)
            srv.reply(200, """{"ok":true}""")
            a.revokeEnrollmentGrant("g")
            assertEquals("/admin/revoke-enrollment-grant", srv.requests.last().path)
        }
    }

    @Test
    fun `admin query and changes`() = run10 {
        TestHttpServer().use { srv ->
            val a = AdminClient(srv.baseUrl, "k")
            srv.reply(200, """{"result":[1,2]}""")
            val r = a.adminQuery("app", dev.parrtdb.wire.Query(table = "t"), includeDeleted = true)
            assertEquals("[1,2]", WireJson.encode(r))
            assertEquals("true", body(srv.requests.last())["includeDeleted"]!!.jsonPrimitive.content)
            srv.reply(200, """{"ops":[],"nextSeq":0,"head":0,"logId":"L"}""")
            a.adminChanges("app", 3, "t", 50)
            assertEquals("limit=50&since=3&table=t", srv.requests.last().query)
            assertEquals("/admin/db/app/changes", srv.requests.last().path)
        }
    }
}
