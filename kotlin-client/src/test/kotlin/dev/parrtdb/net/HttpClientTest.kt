package dev.parrtdb.net

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.http.Fit
import dev.parrtdb.http.OutFormat
import dev.parrtdb.http.RtDbHttpClient
import dev.parrtdb.http.TransformOpts
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.WorkflowStatus
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive

private fun <T> run10(block: suspend () -> T): T = runBlocking { withTimeout(10_000) { block() } }

class HttpClientTest {
    private val txn = Transaction(listOf(Step.Insert("items", jsonObj("a" to jsonStr("b")))))

    private fun parse(body: String) = WireJson.parse(body) as JsonObject

    @Test
    fun `query posts db and query with auth and protocol headers`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(200, """{"result":[{"_id":"1"}]}""")
            val c = RtDbHttpClient(srv.baseUrl + "/", "app", "tok123")
            val r = c.query(Query(table = "items", take = 5))
            assertEquals("""[{"_id":"1"}]""", WireJson.encode(r))
            val req = srv.requests.single()
            assertEquals("POST", req.method)
            assertEquals("/api/query", req.path)
            assertEquals("Bearer tok123", req.header("Authorization"))
            assertEquals("3", req.header("X-Rtdb-Protocol"))
            assertEquals("application/json", req.header("Content-Type"))
            val b = parse(req.body)
            assertEquals("app", b["db"]!!.jsonPrimitive.content)
            assertEquals("items", (b["query"] as JsonObject)["table"]!!.jsonPrimitive.content)
        }
    }

    @Test
    fun `query batch aligns slots and maps slot errors`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(200, """{"results":[{"ok":true,"result":[1]},{"ok":false,"error":{"code":"NOT_FOUND","message":"no table"}}]}""")
            val slots = RtDbHttpClient(srv.baseUrl, "app", "t").queryBatch(listOf(Query(table = "a"), Query(table = "b")))
            assertEquals(2, slots.size)
            assertTrue(slots[0].ok)
            assertEquals("[1]", WireJson.encode(slots[0].result!!))
            assertEquals(ErrorCode.NOT_FOUND, slots[1].error!!.code)
            assertEquals("/api/query-batch", srv.requests.single().path)
        }
    }

    @Test
    fun `mutate sends idempotency key only when non-empty and decodes step results`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(200, """{"results":[{"id":"abc"},null]}""")
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            val res = c.mutate(txn, "key-1")
            assertEquals("abc", res[0].id)
            assertNull(res[1].id)
            assertEquals("key-1", parse(srv.requests[0].body)["idempotencyKey"]!!.jsonPrimitive.content)
            c.mutate(txn, "")
            assertNull(parse(srv.requests[1].body)["idempotencyKey"])
            assertEquals("/api/mutate", srv.requests[1].path)
        }
    }

    @Test
    fun `mutate batch wraps entries and rejects mismatched key count`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(200, """{"results":[{"ok":true,"results":[{"id":"x"}]},{"ok":false,"error":{"code":"CONFLICT","message":"dup"}}]}""")
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            val slots = c.mutateBatch(listOf(txn, txn), listOf("k1", ""))
            assertEquals("x", slots[0].results!![0].id)
            assertEquals(ErrorCode.CONFLICT, slots[1].error!!.code)
            val txns = parse(srv.requests.single().body)["txns"] as kotlinx.serialization.json.JsonArray
            assertEquals("k1", (txns[0] as JsonObject)["idempotencyKey"]!!.jsonPrimitive.content)
            assertNull((txns[1] as JsonObject)["idempotencyKey"])
            assertEquals("/api/mutate-batch", srv.requests.single().path)
            assertFailsWith<IllegalArgumentException> { c.mutateBatch(listOf(txn), listOf("a", "b")) }
        }
    }

    @Test
    fun `mutateWithRetry retries only precondition failures`() = run10 {
        TestHttpServer().use { srv ->
            var n = 0
            srv.respond = { _ ->
                n++
                if (n < 3) Triple(409, """{"code":"PRECONDITION_FAILED","message":"stale"}""", "application/json") else Triple(200, """{"results":[null]}""", "application/json")
            }
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            assertEquals(1, c.mutateWithRetry(5) { txn }.size)
            assertEquals(3, n)
            srv.reply(409, """{"code":"CONFLICT","message":"x"}""")
            assertEquals(ErrorCode.CONFLICT, assertFailsWith<RtDbException> { c.mutateWithRetry(5) { txn } }.code)
        }
    }

    @Test
    fun `error envelope maps code message and retryAfter`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(429, """{"code":"RATE_LIMITED","message":"slow down","retryAfter":2.5}""")
            val e = assertFailsWith<RtDbException> { RtDbHttpClient(srv.baseUrl, "app", "t").query(Query(table = "x")) }
            assertEquals(ErrorCode.RATE_LIMITED, e.code)
            assertEquals("slow down", e.message)
            assertEquals(2.5, e.retryAfter)
        }
    }

    @Test
    fun `non envelope and unknown code bodies become INTERNAL with truncated body`() = run10 {
        TestHttpServer().use { srv ->
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            srv.respond = { Triple(502, "<html>bad gateway</html>", "text/html") }
            val e1 = assertFailsWith<RtDbException> { c.query(Query(table = "x")) }
            assertEquals(ErrorCode.INTERNAL, e1.code)
            assertEquals("HTTP 502: <html>bad gateway</html>", e1.message)
            srv.reply(400, """{"code":"WHAT","message":"m"}""")
            assertTrue(assertFailsWith<RtDbException> { c.query(Query(table = "x")) }.message.startsWith("HTTP 400: "))
            srv.respond = { Triple(500, "z".repeat(300), "text/plain") }
            assertEquals("HTTP 500: " + "z".repeat(200) + "…", assertFailsWith<RtDbException> { c.query(Query(table = "x")) }.message)
        }
    }

    @Test
    fun `read only conflict maps to READ_ONLY`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(409, """{"code":"READ_ONLY","message":"db is read-only"}""")
            val e = assertFailsWith<RtDbException> { RtDbHttpClient(srv.baseUrl, "app", "t").mutate(txn) }
            assertEquals(ErrorCode.READ_ONLY, e.code)
        }
    }

    @Test
    fun `schedule family paths and bodies`() = run10 {
        TestHttpServer().use { srv ->
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            srv.reply(200, """{"id":"s1"}""")
            assertEquals("s1", c.schedule(ScheduleWhen.AfterMs(5), txn, external = true))
            var b = parse(srv.requests.last().body)
            assertEquals("true", b["external"]!!.jsonPrimitive.content)
            assertEquals("/api/schedule", srv.requests.last().path)
            c.schedule(ScheduleWhen.AfterMs(5), txn)
            assertNull(parse(srv.requests.last().body)["external"])

            srv.reply(200, """{"jobs":[{"id":"j1","kind":"oneshot","dueAt":10,"txn":{"steps":[]},"leaseGeneration":3,"leaseDeadlineMs":99}]}""")
            val jobs = c.claimSchedules(limit = 5, leaseMs = 1000)
            assertEquals(3, jobs[0].leaseGeneration)
            b = parse(srv.requests.last().body)
            assertEquals("5", b["limit"]!!.jsonPrimitive.content)
            assertEquals("1000", b["leaseMs"]!!.jsonPrimitive.content)
            assertEquals("/api/schedule/claim", srv.requests.last().path)

            srv.reply(200, "{}")
            c.completeSchedule("a/b", 7)
            assertEquals("/api/schedule/a%2Fb/complete", srv.requests.last().path)
            assertEquals("7", parse(srv.requests.last().body)["lease"]!!.jsonPrimitive.content)
            c.retrySchedule("j1", 7, 500, "boom")
            b = parse(srv.requests.last().body)
            assertEquals("500", b["delayMs"]!!.jsonPrimitive.content)
            assertEquals("boom", b["error"]!!.jsonPrimitive.content)
            c.retrySchedule("j1", 7)
            b = parse(srv.requests.last().body)
            assertNull(b["delayMs"]); assertNull(b["error"])
            c.failSchedule("j1", 7, "dead")
            assertEquals("/api/schedule/j1/fail", srv.requests.last().path)
            assertEquals("dead", parse(srv.requests.last().body)["error"]!!.jsonPrimitive.content)

            srv.reply(200, """{"ok":true}""")
            assertTrue(c.cancelSchedule("j1"))
            assertEquals("/api/schedule/j1/cancel", srv.requests.last().path)
            c.pauseSchedule("j1"); assertEquals("/api/schedule/j1/pause", srv.requests.last().path)
            c.resumeSchedule("j1"); assertEquals("/api/schedule/j1/resume", srv.requests.last().path)

            srv.reply(200, """{"schedules":[]}""")
            assertTrue(c.listSchedules().isEmpty())
            assertEquals("/api/schedules", srv.requests.last().path)
        }
    }

    @Test
    fun `stale lease conflict surfaces as CONFLICT`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(409, """{"code":"CONFLICT","message":"stale lease"}""")
            val e = assertFailsWith<RtDbException> { RtDbHttpClient(srv.baseUrl, "app", "t").completeSchedule("j", 1) }
            assertEquals(ErrorCode.CONFLICT, e.code)
        }
    }

    @Test
    fun `workflow endpoints`() = run10 {
        TestHttpServer().use { srv ->
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            srv.reply(200, """{"id":"w1"}""")
            assertEquals("w1", c.startWorkflow(dev.parrtdb.wire.WorkflowSpec("flow", emptyList())))
            assertEquals("/api/workflows", srv.requests.last().path)
            srv.reply(200, """{"ok":false}""")
            assertEquals(false, c.cancelWorkflow("w1"))
            assertEquals("/api/workflows/w1/cancel", srv.requests.last().path)
            c.signalWorkflow("w1", "go", jsonStr("p"))
            val b = parse(srv.requests.last().body)
            assertEquals("go", b["name"]!!.jsonPrimitive.content)
            assertEquals("p", b["payload"]!!.jsonPrimitive.content)
            srv.reply(200, """{"workflows":[]}""")
            c.listWorkflows(WorkflowStatus.Waiting)
            assertEquals("waiting", parse(srv.requests.last().body)["status"]!!.jsonPrimitive.content)
            assertEquals("/api/workflows/list", srv.requests.last().path)
        }
    }

    @Test
    fun `storage upload download delete metadata and signed url`() = run10 {
        TestHttpServer().use { srv ->
            val c = RtDbHttpClient(srv.baseUrl, "app", "t")
            srv.reply(200, """{"id":"f1","sha256":"abc","size":3,"contentType":"text/plain"}""")
            val up = c.upload("text/plain", byteArrayOf(1, 2, 3))
            assertEquals("f1", up.id)
            assertEquals("/api/storage/app", srv.requests.last().path)
            assertEquals("text/plain", srv.requests.last().header("Content-Type"))
            assertEquals("Bearer t", srv.requests.last().header("Authorization"))

            srv.respond = { Triple(200, "RAW", "application/octet-stream") }
            assertContentEquals("RAW".toByteArray(), c.download("f1"))
            assertEquals("/storage/f1", srv.requests.last().path)

            srv.reply(404, """{"code":"NOT_FOUND","message":"gone"}""")
            assertEquals(ErrorCode.NOT_FOUND, assertFailsWith<RtDbException> { c.download("nope") }.code)

            srv.reply(200, """{"ok":true}""")
            c.deleteFile("f1")
            assertEquals("DELETE", srv.requests.last().method)
            assertEquals("/api/storage/app/f1", srv.requests.last().path)
            srv.reply(200, """{"ok":false}""")
            assertFailsWith<RtDbException> { c.deleteFile("f1") }

            srv.reply(200, """{"id":"f1","sha256":"abc","size":3,"creationTime":9}""")
            assertEquals(9, c.getFileMetadata("f1").creationTime)
            assertEquals("/api/storage/app/f1/metadata", srv.requests.last().path)

            srv.reply(200, """{"url":"http://x/y","expiresAt":5}""")
            c.getSignedUrl("f1", 60)
            assertEquals("/api/storage/app/f1/signed-url", srv.requests.last().path)
            assertEquals("ttlSeconds=60", srv.requests.last().query)
        }
    }

    @Test
    fun `url builders match go`() {
        val c = RtDbHttpClient("http://h:1/", "app", "t")
        assertEquals("http://h:1/storage/f", c.getUrl("f"))
        assertEquals("http://h:1/storage/f", c.transformUrl("f", TransformOpts()))
        assertEquals(
            "http://h:1/storage/f?w=10&h=20&fit=cover&q=80&format=webp",
            c.transformUrl("f", TransformOpts(w = 10, h = 20, fit = Fit.Cover, q = 80, format = OutFormat.WebP)),
        )
    }

    @Test
    fun `changes query string and decode`() = run10 {
        TestHttpServer().use { srv ->
            srv.reply(200, """{"ops":[{"seq":5,"table":"t","docId":"d","kind":"insert","doc":{"a":1},"ts":9}],"nextSeq":5,"head":7,"logId":"L"}""")
            val c = RtDbHttpClient(srv.baseUrl, "my db", "t")
            val r = c.changes(4, "t", 10)
            assertEquals(5, r.nextSeq)
            assertEquals("/api/db/my%20db/changes", srv.requests.last().path)
            assertEquals("limit=10&since=4&table=t", srv.requests.last().query)
            c.changes(0)
            assertEquals("since=0", srv.requests.last().query)
        }
    }

    @Test
    fun `auth me validate and enroll`() = run10 {
        TestHttpServer().use { srv ->
            val c = RtDbHttpClient(srv.baseUrl, "app", "mine")
            srv.reply(200, """{"user":{"kind":"user","email":"a@b.c","name":null}}""")
            assertEquals("a@b.c", c.authMe().email)
            assertEquals("Bearer mine", srv.requests.last().header("Authorization"))
            assertEquals("a@b.c", c.authValidate("other").email)
            assertEquals("/auth/validate", srv.requests.last().path)
            assertEquals("Bearer other", srv.requests.last().header("Authorization"))
            val n = srv.requests.size
            assertEquals(ErrorCode.BAD_REQUEST, assertFailsWith<RtDbException> { c.authValidate("") }.code)
            assertEquals(n, srv.requests.size)

            srv.reply(200, """{"tokenId":"i","token":"T","db":"app","capability":{"type":"parMeshV1","host":"macdaddy"}}""")
            val en = c.enroll("grant-secret")
            assertEquals("macdaddy", en.capability.host)
            val req = srv.requests.last()
            assertEquals("/auth/enroll", req.path)
            assertNull(req.header("Authorization"))
            assertEquals("3", req.header("X-Rtdb-Protocol"))
            assertEquals("grant-secret", parse(req.body)["grant"]!!.jsonPrimitive.content)
            srv.reply(200, """{"tokenId":"i","token":"T","db":"app","capability":{"type":"other","host":"x"}}""")
            assertNotNull(assertFailsWith<dev.parrtdb.wire.WireException> { c.enroll("g") })
        }
    }
}
