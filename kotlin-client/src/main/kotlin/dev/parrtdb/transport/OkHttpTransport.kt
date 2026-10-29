package dev.parrtdb.transport

import dev.parrtdb.ws.WsClosedException
import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.BufferedSink
import okio.source

/**
 * OkHttp-backed transport, the one to use on Android (where `java.net.http` does not exist). OkHttp
 * 4.x is a `compileOnly` dependency of this library: add `com.squareup.okhttp3:okhttp` to your app.
 * Configure timeouts, TLS and proxies on the [client] you pass in.
 */
class OkHttpTransport(private val client: OkHttpClient = OkHttpClient()) : StreamingHttpTransport, WsTransport {
    override suspend fun send(request: TransportRequest): TransportResponse {
        var contentType: String? = null
        val builder = Request.Builder().url(request.url)
        for ((k, v) in request.headers) if (k.equals("Content-Type", ignoreCase = true)) contentType = v else builder.header(k, v)
        val body = request.body ?: if (request.method == "GET" || request.method == "HEAD") null else ByteArray(0)
        builder.method(request.method, body?.toRequestBody(contentType?.toMediaTypeOrNull()))
        val call = client.newCall(builder.build())
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { call.cancel() }
            call.enqueue(
                object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        cont.resumeWithException(e)
                    }

                    override fun onResponse(call: Call, response: Response) {
                        try {
                            response.use { cont.resume(TransportResponse(it.code, it.body?.bytes() ?: ByteArray(0))) }
                        } catch (e: IOException) {
                            cont.resumeWithException(e)
                        }
                    }
                },
            )
        }
    }

    override suspend fun sendStreaming(request: StreamingRequest): StreamingResponse {
        var contentType: String? = null
        val builder = Request.Builder().url(request.url)
        for ((k, v) in request.headers) if (k.equals("Content-Type", ignoreCase = true)) contentType = v else builder.header(k, v)
        val source = request.body
        val body: RequestBody? =
            when {
                source != null -> InputStreamBody(source, contentType?.toMediaTypeOrNull())
                request.method == "GET" || request.method == "HEAD" -> null
                else -> ByteArray(0).toRequestBody(contentType?.toMediaTypeOrNull())
            }
        builder.method(request.method, body)
        val call = client.newCall(builder.build())
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { call.cancel() }
            call.enqueue(
                object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        cont.resumeWithException(e)
                    }

                    // The Response is deliberately left open: closing the returned stream closes it.
                    override fun onResponse(call: Call, response: Response) {
                        cont.resume(StreamingResponse(response.code, response.body?.byteStream() ?: java.io.ByteArrayInputStream(ByteArray(0))))
                    }
                },
            )
        }
    }

    override suspend fun connect(url: String, headers: Map<String, String>): WsSession {
        val builder = Request.Builder().url(url)
        for ((k, v) in headers) builder.header(k, v)
        val ch = Channel<String>(Channel.UNLIMITED)
        val opened = kotlinx.coroutines.CompletableDeferred<Unit>()
        val ws = client.newWebSocket(
            builder.build(),
            object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    opened.complete(Unit)
                }

                override fun onMessage(webSocket: WebSocket, text: String) {
                    ch.trySend(text)
                }

                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    ch.close(WsClosedException(code, reason))
                    webSocket.close(1000, null)
                }

                override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                    ch.close(WsClosedException(code, reason))
                }

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    val e = WsClosedException(1006, t.message ?: "transport error", t)
                    opened.completeExceptionally(e)
                    ch.close(e)
                }
            },
        )
        try {
            opened.await()
        } catch (e: Throwable) {
            ws.cancel()
            throw e
        }
        return OkHttpWsSession(ws, ch)
    }
}

private class OkHttpWsSession(private val ws: WebSocket, private val channel: Channel<String>) : WsSession {
    override val incoming: ReceiveChannel<String> get() = channel

    override suspend fun send(text: String) {
        if (!ws.send(text)) {
            val e = IOException("websocket send failed")
            channel.close(WsClosedException(1006, "send failed", e))
            throw e
        }
    }

    // OkHttp reports every socket loss through onFailure/onClosed, so there is nothing to probe.
    override fun checkAlive() {}

    override suspend fun close() {
        ws.close(1000, null)
        ws.cancel()
    }
}

/** A one-shot request body copied from [input] while sending, so the payload is never materialized. */
private class InputStreamBody(private val input: java.io.InputStream, private val type: okhttp3.MediaType?) : RequestBody() {
    override fun contentType() = type

    override fun isOneShot() = true

    override fun writeTo(sink: BufferedSink) {
        input.use { sink.writeAll(it.source()) }
    }
}
