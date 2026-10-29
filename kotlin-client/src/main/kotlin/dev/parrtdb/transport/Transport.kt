package dev.parrtdb.transport

import kotlinx.coroutines.channels.ReceiveChannel

/** A fully-buffered one-shot HTTP request. [headers] already carries auth, protocol and content type. */
class TransportRequest(val method: String, val url: String, val headers: Map<String, String>, val body: ByteArray?)

/** A fully-buffered HTTP response. Status handling (>= 400 envelopes) is done by the client, not the transport. */
class TransportResponse(val status: Int, val body: ByteArray)

/** Sends one-shot HTTP requests. Transport failures surface as `java.io.IOException`; cancelling the caller cancels the call. */
interface HttpTransport {
    suspend fun send(request: TransportRequest): TransportResponse
}

/** A one-shot HTTP request whose body is read from [body] as it is sent instead of being held in memory. */
class StreamingRequest(val method: String, val url: String, val headers: Map<String, String>, val body: java.io.InputStream?)

/** A response whose [body] is read incrementally. The caller must close [body], which also releases the connection. */
class StreamingResponse(val status: Int, val body: java.io.InputStream)

/**
 * An [HttpTransport] that can also stream request and response bodies. Both bundled transports implement it;
 * a custom [HttpTransport] that does not is rejected by the streaming admin calls with [UnsupportedOperationException].
 */
interface StreamingHttpTransport : HttpTransport {
    /** Request body bytes are pulled from [StreamingRequest.body] while sending, and the response body is not buffered. */
    suspend fun sendStreaming(request: StreamingRequest): StreamingResponse
}

/** Dials WebSocket connections. */
interface WsTransport {
    /** Dials [url]; [headers] are added to the upgrade request. */
    suspend fun connect(url: String, headers: Map<String, String> = emptyMap()): WsSession
}

/**
 * One open WebSocket. Complete text messages (fragments reassembled) arrive on [incoming], which closes
 * with a [dev.parrtdb.ws.WsClosedException] cause when the socket ends.
 */
interface WsSession {
    val incoming: ReceiveChannel<String>

    /** Throws `java.io.IOException` when the socket is down. */
    suspend fun send(text: String)

    /** Liveness probe for the heartbeat: closes [incoming] with a 1006 `WsClosedException` if the socket is gone. */
    fun checkAlive()

    /** Best-effort graceful close that never leaves the socket open. */
    suspend fun close()
}
