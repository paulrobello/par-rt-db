package dev.parrtdb.net

import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * Minimal RFC 6455 server for one client at a time (accept loop supports reconnects): handshake,
 * masked client text frames in, unmasked text frames out (optionally fragmented), ping/close handling.
 */
class FakeWsServer : AutoCloseable {
    private val ss = ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"))
    val port: Int get() = ss.localPort
    val url: String get() = "ws://127.0.0.1:$port/sync"
    val received = LinkedBlockingQueue<String>()
    @Volatile private var out: OutputStream? = null
    @Volatile private var sock: Socket? = null
    @Volatile var connections = 0
    private val acceptor: Thread

    init {
        acceptor = thread(isDaemon = true) {
            while (!ss.isClosed) {
                val s = try { ss.accept() } catch (e: Exception) { return@thread }
                thread(isDaemon = true) { serve(s) }
            }
        }
    }

    private fun serve(s: Socket) {
        try {
            val inp = s.getInputStream()
            val o = s.getOutputStream()
            val headers = StringBuilder()
            while (!headers.endsWith("\r\n\r\n")) {
                val b = inp.read()
                if (b < 0) return
                headers.append(b.toChar())
            }
            val key = Regex("(?i)Sec-WebSocket-Key:\\s*(\\S+)").find(headers)!!.groupValues[1]
            val accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray()))
            o.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: $accept\r\n\r\n".toByteArray())
            o.flush()
            sock = s
            out = o
            connections++
            readFrames(inp, o)
        } catch (e: Exception) {
            // peer closed
        }
    }

    private fun readFrames(inp: InputStream, o: OutputStream) {
        val msg = java.io.ByteArrayOutputStream()
        while (true) {
            val b0 = inp.read()
            if (b0 < 0) return
            val fin = b0 and 0x80 != 0
            val op = b0 and 0x0f
            val b1 = inp.read()
            val masked = b1 and 0x80 != 0
            var len = (b1 and 0x7f).toLong()
            if (len == 126L) len = ((inp.read() shl 8) or inp.read()).toLong()
            else if (len == 127L) { len = 0; repeat(8) { len = (len shl 8) or inp.read().toLong() } }
            val mask = if (masked) ByteArray(4).also { inp.readNBytes(it, 0, 4) } else ByteArray(4)
            val payload = inp.readNBytes(len.toInt())
            for (i in payload.indices) payload[i] = (payload[i].toInt() xor mask[i % 4].toInt()).toByte()
            when (op) {
                0x8 -> { writeFrame(o, 0x8, payload); return }
                0x9 -> writeFrame(o, 0xA, payload)
                0x1, 0x0 -> {
                    msg.write(payload)
                    if (fin) { received.add(msg.toString(Charsets.UTF_8)); msg.reset() }
                }
            }
        }
    }

    private fun writeFrame(o: OutputStream, opcode: Int, payload: ByteArray, fin: Boolean = true) {
        synchronized(o) {
            o.write((if (fin) 0x80 else 0) or opcode)
            when {
                payload.size < 126 -> o.write(payload.size)
                payload.size < 65536 -> { o.write(126); o.write(payload.size shr 8); o.write(payload.size and 0xff) }
                else -> { o.write(127); for (i in 7 downTo 0) o.write((payload.size.toLong() shr (8 * i)).toInt() and 0xff) }
            }
            o.write(payload)
            o.flush()
        }
    }

    /** Sends one complete text message. */
    fun send(text: String) = writeFrame(out ?: error("no client"), 0x1, text.toByteArray())

    /** Sends [text] as fragments of at most [chunk] bytes (continuation frames). */
    fun sendFragmented(text: String, chunk: Int) {
        val o = out ?: error("no client")
        val bytes = text.toByteArray()
        var i = 0
        while (i < bytes.size) {
            val end = minOf(bytes.size, i + chunk)
            writeFrame(o, if (i == 0) 0x1 else 0x0, bytes.copyOfRange(i, end), fin = end == bytes.size)
            i = end
        }
    }

    /** Drops the TCP connection without a close frame. */
    fun dropClient() {
        runCatching { sock?.close() }
    }

    /** Blocks (bounded) for the next client frame. */
    fun next(timeoutMs: Long = 5000): String =
        received.poll(timeoutMs, TimeUnit.MILLISECONDS) ?: error("timed out waiting for a client frame")

    override fun close() {
        runCatching { ss.close() }
        dropClient()
    }
}
