package dev.parrtdb.http

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.PROTOCOL_VERSION
import dev.parrtdb.wire.WireJson
import dev.parrtdb.transport.HttpTransport
import dev.parrtdb.transport.TransportRequest
import dev.parrtdb.transport.TransportResponse
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * The one-shot HTTP seam shared by [RtDbHttpClient] and the admin client. Every request
 * carries `Authorization: Bearer <token>` (unless [bearer] is null) and `X-Rtdb-Protocol`;
 * every status >= 400 response decodes the `{code,message,retryAfter}` envelope into an
 * [RtDbException].
 */
internal class HttpCore(baseUrl: String, private val token: String, private val http: HttpTransport) {
    val baseUrl: String = baseUrl.trimEnd('/')

    suspend fun execute(
        method: String,
        path: String,
        body: ByteArray?,
        contentType: String?,
        bearer: String? = token,
    ): TransportResponse {
        val headers = LinkedHashMap<String, String>()
        if (bearer != null) headers["Authorization"] = "Bearer $bearer"
        headers["X-Rtdb-Protocol"] = PROTOCOL_VERSION.toString()
        if (contentType != null) headers["Content-Type"] = contentType
        return http.send(TransportRequest(method, baseUrl + path, headers, body))
    }

    /** Sends and returns the 2xx body bytes undecoded. */
    suspend fun bytes(
        method: String,
        path: String,
        body: ByteArray? = null,
        contentType: String? = null,
        bearer: String? = token,
    ): ByteArray {
        val resp = execute(method, path, body, contentType, bearer)
        if (resp.status >= 400) throw envelopeError(resp.status, resp.body)
        return resp.body
    }

    /** Sends an optional JSON body and decodes the 2xx JSON response. */
    suspend fun json(method: String, path: String, body: JsonElement? = null, bearer: String? = token): JsonElement =
        WireJson.parse(String(jsonBytes(method, path, body, bearer), Charsets.UTF_8))

    /** Sends an optional JSON body and ignores the 2xx response body. */
    suspend fun discard(method: String, path: String, body: JsonElement? = null) {
        jsonBytes(method, path, body, token)
    }

    private suspend fun jsonBytes(method: String, path: String, body: JsonElement?, bearer: String?): ByteArray =
        bytes(method, path, body?.let { WireJson.encode(it).toByteArray(Charsets.UTF_8) }, if (body != null) "application/json" else null, bearer)
}

private fun truncate(s: String, n: Int): String = if (s.length <= n) s else s.substring(0, n) + "…"

/**
 * Decodes a non-2xx body into an [RtDbException]. A body that is not a recognised
 * envelope (or carries an unknown code) becomes INTERNAL `HTTP <status>: <truncated body>`.
 */
internal fun envelopeError(status: Int, data: ByteArray): RtDbException {
    val text = String(data, Charsets.UTF_8)
    val env = try { WireJson.parse(text) } catch (e: Exception) { null }
    if (env is JsonObject) {
        val code = (env["code"] as? JsonPrimitive)?.takeIf { it.isString }?.content
        val ec = code?.let { ErrorCode.fromWire(it) }
        if (ec != null) {
            val msg = (env["message"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: ""
            val ra = (env["retryAfter"] as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toDoubleOrNull()
            return RtDbException(ec, msg, ra)
        }
    }
    return RtDbException(ErrorCode.INTERNAL, "HTTP $status: ${truncate(text, 200)}")
}

/** Maps a slot-level error object (`{code,message,retryAfter?}`) to an exception; unknown codes become INTERNAL. */
internal fun slotError(e: JsonElement): RtDbException {
    val o = e as? JsonObject
    val code = (o?.get("code") as? JsonPrimitive)?.content ?: ""
    val msg = (o?.get("message") as? JsonPrimitive)?.content ?: ""
    val ra = (o?.get("retryAfter") as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toDoubleOrNull()
    val ec = ErrorCode.fromWire(code) ?: return RtDbException(ErrorCode.INTERNAL, "$code: $msg")
    return RtDbException(ec, msg, ra)
}
