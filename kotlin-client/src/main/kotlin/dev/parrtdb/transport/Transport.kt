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
