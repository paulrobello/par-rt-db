package dev.parrtdb.net

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.ws.RtDbWsClient
import dev.parrtdb.ws.WsClosedException
import dev.parrtdb.ws.WsOptions
import dev.parrtdb.ws.syncUrl
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive

private fun <T> run10(block: suspend kotlinx.coroutines.CoroutineScope.() -> T): T =
    runBlocking(kotlinx.coroutines.Dispatchers.Default) { withTimeout(10_000) { block() } }

private const val AUTH_OK = """{"type":"authOk","user":{"kind":"machine","email":null,"name":null},"protocolVersion":3}"""

class WsClientTest {
    private fun obj(s: String) = WireJson.parse(s) as JsonObject
    private fun type(s: String) = obj(s)["type"]!!.jsonPrimitive.content

    private fun connected(srv: FakeWsServer, opts: WsOptions = WsOptions(heartbeatMs = 600_000)): RtDbWsClient =
        RtDbWsClient(srv.url, "app", { "tok" }, opts)

    private suspend fun RtDbWsClient.connectAuthed(srv: FakeWsServer) {
        kotlinx.coroutines.coroutineScope {
            val d = async { connect() }
            val auth = obj(srv.next())
            assertEquals("auth", auth["type"]!!.jsonPrimitive.content)
            assertEquals("tok", auth["token"]!!.jsonPrimitive.content)
            assertEquals("app", auth["db"]!!.jsonPrimitive.content)
            assertEquals("3", auth["protocolVersion"]!!.jsonPrimitive.content)
            srv.send(AUTH_OK)
            d.await()
        }
    }

    @Test
    fun `sync url maps scheme`() {
        assertEquals("ws://h:1/sync", syncUrl("http://h:1/"))
        assertEquals("wss://h/sync", syncUrl("https://h"))
    }

    @Test
    fun `auth error surfaces as RtDbException`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            val d = async { runCatching { c.connect() } }
            srv.next()
            srv.send("""{"type":"authErr","error":{"code":"UNAUTHORIZED","message":"bad token"}}""")
            val e = d.await().exceptionOrNull() as RtDbException
            assertEquals(ErrorCode.UNAUTHORIZED, e.code)
            c.close()
        }
    }

    @Test
    fun `subscribe emits updates and unsubscribes on cancellation`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val got = async { c.subscribe(Query(table = "items")).take(2).toList() }
            val sub = obj(srv.next())
            assertEquals("subscribe", sub["type"]!!.jsonPrimitive.content)
            val qid = sub["queryId"]!!.jsonPrimitive.content
            assertEquals("items", (sub["query"] as JsonObject)["table"]!!.jsonPrimitive.content)
            srv.send("""{"type":"queryUpdate","queryId":"other","result":[99]}""")
            srv.send("""{"type":"queryUpdate","queryId":"$qid","result":[1]}""")
            srv.send("""{"type":"queryUpdate","queryId":"$qid","result":[1,2]}""")
            assertEquals(listOf("[1]", "[1,2]"), got.await().map { WireJson.encode(it) })
            val unsub = obj(srv.next())
            assertEquals("unsubscribe", unsub["type"]!!.jsonPrimitive.content)
            assertEquals(qid, unsub["queryId"]!!.jsonPrimitive.content)
            c.close()
        }
    }

    @Test
    fun `subscribe error ends flow with exception`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val r = async { runCatching { c.subscribe(Query(table = "nope")).first() } }
            val qid = obj(srv.next())["queryId"]!!.jsonPrimitive.content
            srv.send("""{"type":"subscribeErr","queryId":"$qid","error":{"code":"NOT_FOUND","message":"no such table"}}""")
            assertEquals(ErrorCode.NOT_FOUND, (r.await().exceptionOrNull() as RtDbException).code)
            c.close()
        }
    }

    @Test
    fun `mutate correlates ok and err by id`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val txn = Transaction(listOf(Step.Insert("items", jsonObj("a" to jsonStr("b")))))
            val ok = async { c.mutate(txn, "idem") }
            val m = obj(srv.next())
            assertEquals("mutate", m["type"]!!.jsonPrimitive.content)
            assertEquals("idem", m["idempotencyKey"]!!.jsonPrimitive.content)
            srv.send("""{"type":"mutateOk","mutId":"${m["mutId"]!!.jsonPrimitive.content}","results":[{"id":"new1"}]}""")
            assertEquals("new1", ok.await()[0].id)

            val bad = async { runCatching { c.mutate(txn) } }
            val m2 = obj(srv.next())
            srv.send("""{"type":"mutateErr","mutId":"${m2["mutId"]!!.jsonPrimitive.content}","error":{"code":"READ_ONLY","message":"frozen"}}""")
            assertEquals(ErrorCode.READ_ONLY, (bad.await().exceptionOrNull() as RtDbException).code)
            c.close()
        }
    }

    @Test
    fun `fragmented text frames are reassembled`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val got = async { c.subscribe(Query(table = "items")).first() }
            val qid = obj(srv.next())["queryId"]!!.jsonPrimitive.content
            val big = (1..200).joinToString(",", "[", "]") { """{"n":$it}""" }
            srv.sendFragmented("""{"type":"queryUpdate","queryId":"$qid","result":$big}""", 37)
            assertEquals(big, WireJson.encode(got.await()))
            c.close()
        }
    }

    @Test
    fun `schedule and workflow acks`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val txn = Transaction(emptyList())
            val s = async { c.schedule(ScheduleWhen.AfterMs(5), txn) }
            val f = obj(srv.next())
            assertEquals("schedule", f["type"]!!.jsonPrimitive.content)
            srv.send("""{"type":"scheduleOk","scheduleId":"${f["scheduleId"]!!.jsonPrimitive.content}","id":"job9"}""")
            assertEquals("job9", s.await())

            val cancel = async { c.cancelSchedule("job9") }
            val cf = obj(srv.next())
            assertEquals("cancelSchedule", cf["type"]!!.jsonPrimitive.content)
            assertEquals("job9", cf["id"]!!.jsonPrimitive.content)
            srv.send("""{"type":"scheduleAck","scheduleId":"${cf["scheduleId"]!!.jsonPrimitive.content}","ok":true}""")
            assertTrue(cancel.await())

            val sig = async { runCatching { c.signalWorkflow("w1", "go") } }
            val wf = obj(srv.next())
            assertEquals("signalWorkflow", wf["type"]!!.jsonPrimitive.content)
            srv.send("""{"type":"workflowAck","workflowId":"${wf["workflowId"]!!.jsonPrimitive.content}","ok":false,"error":{"code":"FORBIDDEN","message":"no"}}""")
            assertEquals(ErrorCode.FORBIDDEN, (sig.await().exceptionOrNull() as RtDbException).code)
            c.close()
        }
    }

    @Test
    fun `presence folds snapshot then delta and leaves on cancel`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val got = async { c.presence("lobby", jsonStr("hi")).take(2).toList() }
            val join = obj(srv.next())
            assertEquals("presence", join["type"]!!.jsonPrimitive.content)
            assertEquals("lobby", join["room"]!!.jsonPrimitive.content)
            val m1 = """{"connectionId":"c1","user":{"kind":"user","email":null,"name":null},"state":1}"""
            val m2 = """{"connectionId":"c2","user":{"kind":"user","email":null,"name":null},"state":2}"""
            srv.send("""{"type":"presenceSnapshot","room":"lobby","members":[$m1]}""")
            srv.send("""{"type":"presenceDelta","room":"lobby","seq":5,"joined":[$m2]}""")
            val lists = got.await()
            assertEquals(listOf("c1"), lists[0].map { it.connectionId })
            assertEquals(listOf("c1", "c2"), lists[1].map { it.connectionId })
            assertEquals("leavePresence", obj(srv.next())["type"]!!.jsonPrimitive.content)
            c.close()
        }
    }

    @Test
    fun `reconnect re-authenticates and replays subscriptions`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv, WsOptions(backoffBaseMs = 20, backoffMaxMs = 50, heartbeatMs = 100))
            c.connectAuthed(srv)
            val got = async { c.subscribe(Query(table = "items")).take(2).toList() }
            val qid = obj(srv.next())["queryId"]!!.jsonPrimitive.content
            srv.send("""{"type":"queryUpdate","queryId":"$qid","result":[1]}""")
            srv.dropClient()
            assertEquals("auth", type(srv.next()))
            srv.send(AUTH_OK)
            val replay = obj(srv.next())
            assertEquals("subscribe", replay["type"]!!.jsonPrimitive.content)
            assertEquals(qid, replay["queryId"]!!.jsonPrimitive.content)
            srv.send("""{"type":"queryUpdate","queryId":"$qid","result":[2]}""")
            assertEquals(listOf("[1]", "[2]"), got.await().map { WireJson.encode(it) })
            c.close()
        }
    }

    @Test
    fun `pending request fails when connection drops and close is clean`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv, WsOptions(backoffBaseMs = 20, backoffMaxMs = 50, heartbeatMs = 100))
            c.connectAuthed(srv)
            val r = async { runCatching { c.mutate(Transaction(emptyList())) } }
            srv.next()
            srv.dropClient()
            assertTrue(r.await().exceptionOrNull() is WsClosedException)
            c.close()
            c.close()
            assertFailsWith<IllegalStateException> { c.connect() }
        }
    }

    @Test
    fun `heartbeat sends ping frames`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv, WsOptions(heartbeatMs = 50))
            c.connectAuthed(srv)
            assertEquals("ping", type(srv.next()))
            c.close()
        }
    }

    private fun op(seq: Long) = """{"seq":$seq,"table":"t","docId":"d$seq","kind":"insert","doc":{"n":$seq},"ts":1}"""

    private fun events(fid: String, epoch: String, seqs: List<Long>, head: Long) =
        """{"type":"feedEvents","feedId":"$fid","feedEpoch":"$epoch","ops":[${seqs.joinToString(",") { op(it) }}],"head":$head}"""

    @Test
    fun `watchChanges sends tables, acks batches and unsubscribes on cancel`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val got = async { c.watchChanges(tables = listOf("a", "b"), since = 5, feedEpoch = "e1").take(2).toList() }
            val sub = obj(srv.next())
            assertEquals("feedSubscribe", sub["type"]!!.jsonPrimitive.content)
            assertEquals("5", sub["since"]!!.jsonPrimitive.content)
            assertEquals("e1", sub["feedEpoch"]!!.jsonPrimitive.content)
            assertEquals(2, (sub["tables"] as kotlinx.serialization.json.JsonArray).size)
            val fid = sub["feedId"]!!.jsonPrimitive.content
            srv.send(events(fid, "e1", listOf(6, 7), 7))
            val ack = obj(srv.next())
            assertEquals("feedAck", ack["type"]!!.jsonPrimitive.content)
            assertEquals("7", ack["seq"]!!.jsonPrimitive.content)
            srv.send(events(fid, "e1", listOf(8), 8))
            val batches = got.await()
            assertEquals(listOf(6L, 7L), batches[0].ops.map { it.seq })
            assertEquals("feedUnsubscribe", type(srv.next()))
            c.close()
        }
    }

    @Test
    fun `watchChanges omits tables when empty and resumes from last seq with dedupe after reconnect`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv, WsOptions(backoffBaseMs = 20, backoffMaxMs = 50, heartbeatMs = 100))
            c.connectAuthed(srv)
            val got = async { c.watchChanges().take(2).toList() }
            val sub = obj(srv.next())
            assertTrue("tables" !in sub && "since" !in sub && "feedEpoch" !in sub)
            val fid = sub["feedId"]!!.jsonPrimitive.content
            srv.send(events(fid, "e1", emptyList(), 10))
            srv.send(events(fid, "e1", listOf(11, 12), 12))
            assertEquals("feedAck", type(srv.next()))
            srv.dropClient()
            assertEquals("auth", type(srv.next()))
            srv.send(AUTH_OK)
            val re = obj(srv.next())
            assertEquals("feedSubscribe", re["type"]!!.jsonPrimitive.content)
            assertEquals(fid, re["feedId"]!!.jsonPrimitive.content)
            assertEquals("12", re["since"]!!.jsonPrimitive.content)
            assertEquals("e1", re["feedEpoch"]!!.jsonPrimitive.content)
            // At-least-once redelivery: 12 is a duplicate and must be dropped.
            srv.send(events(fid, "e1", listOf(12, 13), 13))
            val batches = got.await()
            assertEquals(listOf(11L, 12L), batches[0].ops.map { it.seq })
            assertEquals(listOf(13L), batches[1].ops.map { it.seq })
            c.close()
        }
    }

    @Test
    fun `watchChanges surfaces CURSOR_EXPIRED and does not retry`() = run10 {
        FakeWsServer().use { srv ->
            val c = connected(srv)
            c.connectAuthed(srv)
            val r = async { runCatching { c.watchChanges(since = 1).toList() } }
            val fid = obj(srv.next())["feedId"]!!.jsonPrimitive.content
            srv.send("""{"type":"feedErr","feedId":"$fid","error":{"code":"CURSOR_EXPIRED","message":"resync"}}""")
            assertEquals(ErrorCode.CURSOR_EXPIRED, (r.await().exceptionOrNull() as RtDbException).code)
            assertTrue(srv.received.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS) == null, "no resubscribe or unsubscribe after feedErr")
            c.close()
        }
    }
}
