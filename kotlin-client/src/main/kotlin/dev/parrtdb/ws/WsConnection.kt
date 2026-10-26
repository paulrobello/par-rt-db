package dev.parrtdb.ws

import dev.parrtdb.http.await
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.util.concurrent.CompletionStage
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull

/** A WebSocket connection ended; [code] is the close status (1006 for abnormal loss). */
class WsClosedException(val code: Int, val reason: String, cause: Throwable? = null) :
    java.io.IOException("websocket closed ($code): $reason", cause)

/**
 * One open `java.net.http.WebSocket` with complete text messages delivered on [incoming].
 * Fragmented frames (`last == false`) are reassembled before delivery. The channel closes with a
 * [WsClosedException] cause when the socket ends.
 */
internal class WsConnection private constructor(private val ws: WebSocket, val incoming: Channel<String>) {
    private val writeLock = kotlinx.coroutines.sync.Mutex()

    suspend fun send(text: String) {
        try {
            writeLock.withLockCompat { ws.sendText(text, true).await() }
        } catch (e: java.io.IOException) {
            incoming.close(WsClosedException(1006, e.message ?: "send failed", e))
            throw e
        }
    }

    /**
     * Liveness probe for the heartbeat. The JDK client does not always call the listener when the peer
     * drops the TCP connection without a close frame, so a closed input or output side is reported here.
     */
    fun checkAlive() {
        if (ws.isInputClosed || ws.isOutputClosed) incoming.close(WsClosedException(1006, "connection lost"))
    }

    /** Best-effort graceful close, then abort so a silent peer cannot hold the socket open. */
    suspend fun close() {
        withTimeoutOrNull(1000) { runCatching { ws.sendClose(WebSocket.NORMAL_CLOSURE, "").await() } }
        ws.abort()
    }

    private class Listener(private val out: Channel<String>) : WebSocket.Listener {
        private val buf = StringBuilder()

        override fun onOpen(webSocket: WebSocket) {
            webSocket.request(1)
        }

        override fun onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            buf.append(data)
            if (last) {
                val text = buf.toString()
                buf.setLength(0)
                out.trySend(text)
            }
            webSocket.request(1)
            return null
        }

        override fun onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
            out.close(WsClosedException(statusCode, reason))
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "")
            return null
        }

        override fun onError(webSocket: WebSocket, error: Throwable) {
            out.close(WsClosedException(1006, error.message ?: "transport error", error))
        }
    }

    companion object {
        /** Dials [uri]; [headers] are added to the upgrade request. */
        suspend fun dial(http: HttpClient, uri: URI, headers: Map<String, String> = emptyMap()): WsConnection {
            val ch = Channel<String>(Channel.UNLIMITED)
            val builder = http.newWebSocketBuilder()
            for ((k, v) in headers) builder.header(k, v)
            val ws = builder.buildAsync(uri, Listener(ch)).await()
            return WsConnection(ws, ch)
        }
    }
}

private suspend inline fun <T> kotlinx.coroutines.sync.Mutex.withLockCompat(crossinline body: suspend () -> T): T {
    lock()
    try {
        return body()
    } finally {
        unlock()
    }
}
