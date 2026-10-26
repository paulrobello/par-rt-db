package dev.parrtdb.corpus

import dev.parrtdb.admin.Directive
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.inmemory.InMemoryClient
import dev.parrtdb.inmemory.InMemoryOptions
import dev.parrtdb.inmemory.evalQuery
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.File

/**
 * The semantics-corpus runner (wire-corpus/README.md "How a runner executes a case"): fresh engine
 * per case, push (or pushError), seed with `$id` labels, `$idRef`/`$prev` substitution, run the
 * op, compare after `normalize` / `unordered` / numeric tolerance, then the optional `then` read.
 */
object SemanticsRunner {
    sealed interface Outcome {
        data object Executed : Outcome

        data class Skipped(val reason: String) : Outcome
    }

    class CaseFailure(message: String) : AssertionError(message)

    fun caseFiles(): List<File> =
        File(CorpusPaths.root, "semantics").listFiles { f -> f.isFile && f.name.endsWith(".json") }!!.sortedBy { it.name }

    private fun fail(name: String, msg: String): Nothing = throw CaseFailure("$name: $msg")

    private fun newClient(): InMemoryClient {
        var clock = 1_700_000_000_000L
        return InMemoryClient(InMemoryOptions(now = { ++clock }, random = { 0.0 }))
    }

    fun run(file: File): Outcome {
        val stem = file.name.removeSuffix(".json")
        val root =
            try {
                WireJson.parse(file.readText(Charsets.UTF_8)) as? JsonObject ?: fail(stem, "fixture is not a JSON object")
            } catch (e: dev.parrtdb.wire.WireException) {
                fail(stem, "fixture did not parse: ${e.message}")
            }
        val name = (root["name"] as? JsonPrimitive)?.content ?: fail(stem, "missing `name`")
        if (name != stem) fail(stem, "case `name` must equal the filename stem")
        (root["skip"] as? JsonObject)?.get("kotlin")?.let { return Outcome.Skipped((it as? JsonPrimitive)?.content ?: "") }

        val client = newClient()
        val schema = root["schema"] ?: fail(name, "missing schema")

        root["pushError"]?.let { pe ->
            for (stray in listOf("seed", "op", "then", "expect")) {
                if (root.containsKey(stray)) fail(name, "a pushError case must not carry `$stray`")
            }
            val want = ((pe as? JsonObject)?.get("code") as? JsonPrimitive)?.content ?: fail(name, "pushError.code missing")
            val err = runCatching { client.pushSchema(schema) }.exceptionOrNull() ?: fail(name, "pushError case: the push must fail")
            assertCode(name, err, want)
            return Outcome.Executed
        }

        try {
            client.pushSchema(schema)
        } catch (e: RtDbException) {
            fail(name, "push_schema: $e")
        }
        val singleTable = ((schema as? JsonObject)?.get("tables") as? JsonObject)?.takeIf { it.size == 1 }?.keys?.first()
        val ids = HashMap<String, String>()
        seed(client, name, root["seed"], singleTable, ids)

        if ((root["dbReadOnly"] as? JsonPrimitive)?.content == "true") client.setReadOnly(true)

        val expect = root["expect"] ?: fail(name, "missing expect")
        val wantErr = errorCode(expect)
        val normalize = normalizeKeys(root)
        val op = root["op"] as? JsonObject ?: fail(name, "missing op")
        val result: JsonElement =
            try {
                executeOp(client, name, op, ids)
            } catch (e: RtDbException) {
                if (wantErr == null) fail(name, "unexpected op error: $e")
                assertCode(name, e, wantErr)
                return Outcome.Executed
            }
        if (wantErr != null) fail(name, "expected error $wantErr, got success ${Compare.canonical(result)}")
        assertResult(name, result, expect, normalize, flag(root, "unordered"), flag(root, "expect_next_cursor"))

        (root["then"] as? JsonObject)?.let { then ->
            val q = substitute(then["query"] ?: fail(name, "then requires query"), ids, null)
            val actual = evalQuery(client.store, parseQuery(name, q))
            val keys = if (then.containsKey("normalize")) normalizeKeys(then) else normalize
            assertResult(name, actual, then["expect"] ?: fail(name, "then requires expect"), keys, flag(then, "unordered"), flag(then, "expect_next_cursor"))
        }
        return Outcome.Executed
    }

    private fun flag(o: JsonObject, key: String): Boolean = (o[key] as? JsonPrimitive)?.content == "true"

    private fun normalizeKeys(o: JsonObject): List<String> =
        (o["normalize"] as? JsonArray)?.map { (it as JsonPrimitive).content } ?: Compare.defaultNormalize

    private fun errorCode(expect: JsonElement): String? =
        (((expect as? JsonObject)?.get("error") as? JsonObject)?.get("code") as? JsonPrimitive)?.takeIf { it.isString }?.content

    private fun assertCode(name: String, err: Throwable, want: String) {
        val re = err as? RtDbException ?: fail(name, "error is not an RtDbException: $err")
        if (re.code.wire != want) fail(name, "error code mismatch: want $want, got ${re.code.wire} (engine message: ${re.message})")
        if (ErrorCode.fromWire(want) == null) fail(name, "unknown wire error code $want")
    }

    private fun seed(client: InMemoryClient, name: String, seedRaw: JsonElement?, singleTable: String?, ids: MutableMap<String, String>) {
        val entries = seedRaw as? JsonArray ?: fail(name, "seed must be an array")
        entries.forEachIndexed { i, entry ->
            val obj = entry as? JsonObject ?: fail(name, "seed #$i must be a JSON object")
            val docV = obj["doc"]
            val table: String
            val doc: JsonElement
            var label: String? = null
            if (docV is JsonObject) {
                table = (obj["table"] as? JsonPrimitive)?.content ?: singleTable ?: fail(name, "seed #$i: wrapped entry without `table` requires a single-table schema")
                label = (obj["\$id"] as? JsonPrimitive)?.content
                doc = JsonObject(docV.filterKeys { it != "\$id" && it != "table" })
            } else {
                table = singleTable ?: fail(name, "seed #$i: plain-doc seed requires a single-table schema")
                doc = obj
            }
            val substituted = substitute(doc, ids, null)
            val results =
                try {
                    client.mutate(Transaction(listOf(Step.Insert(table, substituted))))
                } catch (e: RtDbException) {
                    fail(name, "seed #$i into '$table': $e")
                }
            val id = results.firstOrNull()?.id ?: fail(name, "seed #$i: expected an Insert result")
            if (label != null) ids[label] = id
        }
    }

    private fun substitute(v: JsonElement, ids: Map<String, String>, prev: String?): JsonElement =
        when (v) {
            is JsonObject -> {
                val ref = if (v.size == 1) v["\$idRef"] else null
                if (ref != null) {
                    val label = (ref as? JsonPrimitive)?.takeIf { it.isString }?.content ?: throw CaseFailure("\$idRef label must be a string")
                    jsonStr(ids[label] ?: throw CaseFailure("\$idRef references unknown seed label '$label'"))
                } else {
                    JsonObject(
                        v.mapValues { (k, x) ->
                            if (k == "cursor" && x is JsonPrimitive && x.isString && x.content == "\$prev") {
                                jsonStr(prev ?: throw CaseFailure("\$prev sentinel resolved outside the two-pass query run"))
                            } else {
                                substitute(x, ids, prev)
                            }
                        },
                    )
                }
            }
            is JsonArray -> JsonArray(v.map { substitute(it, ids, prev) })
            else -> v
        }

    private fun parseQuery(name: String, q: JsonElement): Query =
        try {
            Query.fromJson(q)
        } catch (e: WireException) {
            fail(name, "query does not parse: ${e.message}")
        }

    private fun executeOp(client: InMemoryClient, name: String, op: JsonObject, ids: Map<String, String>): JsonElement {
        op["txn"]?.let { raw ->
            val txn =
                try {
                    Transaction.fromJson(substitute(raw, ids, null))
                } catch (e: WireException) {
                    throw RtDbException(ErrorCode.BAD_REQUEST, "malformed transaction: ${e.message}")
                }
            return stepResultsToWire(client.mutate(txn))
        }
        op["query"]?.let { raw ->
            var prev: String? = null
            val cursor = (((raw as? JsonObject)?.get("paginate") as? JsonObject)?.get("cursor") as? JsonPrimitive)?.takeIf { it.isString }?.content
            if (cursor == "\$prev") {
                val o = raw
                val pag = JsonObject((o["paginate"] as JsonObject).filterKeys { it != "cursor" })
                val firstQuery = JsonObject(o + ("paginate" to pag))
                val first = evalQuery(client.store, parseQuery(name, substitute(firstQuery, ids, null)))
                prev = ((first as? JsonObject)?.get("nextCursor") as? JsonPrimitive)?.content ?: fail(name, "\$prev: first page has no nextCursor")
            }
            return evalQuery(client.store, parseQuery(name, substitute(raw, ids, prev)))
        }
        op["migrate"]?.let { raw ->
            val req = substitute(raw, ids, null) as? JsonObject ?: fail(name, "op.migrate must be an object")
            val directives =
                (req["directives"] as? JsonArray ?: fail(name, "op.migrate.directives missing")).map {
                    try {
                        Directive.fromJson(it)
                    } catch (e: WireException) {
                        fail(name, "directive does not parse: ${e.message}")
                    }
                }
            val dryRun = (req["dryRun"] as? JsonPrimitive)?.content == "true"
            return client.applyMigration(directives, dryRun).toJson()
        }
        fail(name, "op must carry `query`, `txn`, or `migrate`")
    }

    /** The README's txn expect shape: one untagged entry per step. */
    private fun stepResultsToWire(results: List<StepResult>): JsonElement =
        JsonArray(
            results.map { r ->
                when {
                    r.ids != null ->
                        jsonObj(
                            "ids" to JsonArray(r.ids.map { jsonStr(it) }),
                            "inserted" to r.insertedMany?.let { l -> JsonArray(l.map { jsonBool(it) }) },
                        )
                    r.id != null && r.inserted != null -> jsonObj("id" to jsonStr(r.id), "inserted" to jsonBool(r.inserted))
                    r.id != null -> jsonObj("id" to jsonStr(r.id))
                    r.patched != null -> jsonObj("patched" to jsonNum(r.patched), "truncated" to jsonBool(r.truncated == true))
                    r.deleted != null -> jsonObj("deleted" to jsonNum(r.deleted), "truncated" to jsonBool(r.truncated == true))
                    r.scheduleId != null -> jsonObj("scheduleId" to jsonStr(r.scheduleId))
                    r.cancelled != null -> jsonObj("cancelled" to jsonBool(r.cancelled))
                    r.workflowId != null -> jsonObj("workflowId" to jsonStr(r.workflowId))
                    else -> JsonNull
                }
            },
        )

    private fun assertResult(name: String, actual: JsonElement, expect: JsonElement, keys: List<String>, unordered: Boolean, expectNextCursor: Boolean) {
        var normalize: List<String> = keys
        if (expectNextCursor) {
            val obj = actual as? JsonObject ?: fail(name, "nextCursor assert requires an object result, got ${Compare.canonical(actual)}")
            if (!obj.containsKey("nextCursor")) fail(name, "nextCursor presence mismatch (got false, want true)")
            normalize = keys + "nextCursor"
        }
        Compare.check(actual, expect, normalize, unordered)?.let { fail(name, it) }
    }
}
