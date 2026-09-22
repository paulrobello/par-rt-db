import Foundation

// MARK: - /admin/stream op-feed (mirrors rust stream_admin / ts streamAdmin)

//
// The realtime op feed: up to 200 ring events replayed per (re)connection,
// then live ops interleaved with ~1s gauge snapshots. Frames this client
// cannot parse are skipped, not fatal (an unknown `kind` from a newer server
// must not kill an operator's tail). Every reconnect replays up to 200 ring
// events, so duplicates after a blip are expected — OpEvent carries no
// sequence to dedup on.

/// One frame of the `/admin/stream` op feed. Mirrors ts-client's
/// `AdminStreamFrame` union / rust `wire::admin::AdminStreamFrame`.
public enum AdminStreamFrame: Equatable, Sendable {
    /// A document op event (replay then live).
    case op(OpEvent)
    /// The ~1s server metrics snapshot.
    case gauges(MetricsSnapshot)

    /// Decode one text frame, or `nil` for anything unknown or unparseable:
    /// unknown `kind`, malformed JSON, or a known kind with an invalid
    /// payload (the stricter of ts-client's `parseAdminStreamFrame` and the
    /// rust union decode). Skipping — never fatal.
    static func decode(_ text: String) -> AdminStreamFrame? {
        guard let data = text.data(using: .utf8) else { return nil }
        let decoder = JSONDecoder()
        guard
            let probe = try? decoder.decode(
                FrameProbe.self, from: Data(data)
            )
        else { return nil }
        switch probe.kind {
        case "op":
            guard let envelope = try? decoder.decode(OpEnvelope.self, from: data) else {
                return nil
            }
            return .op(envelope.event)
        case "gauges":
            guard let envelope = try? decoder.decode(GaugesEnvelope.self, from: data) else {
                return nil
            }
            return .gauges(envelope.gauges)
        default:
            return nil
        }
    }

    private struct FrameProbe: Decodable {
        let kind: String
    }

    private struct OpEnvelope: Decodable {
        let event: OpEvent
    }

    private struct GaugesEnvelope: Decodable {
        let gauges: MetricsSnapshot
    }
}

// MARK: - Stream pump

/// The `/admin/stream` connection loop. Built by
/// `RtDbAdminClient.streamAdmin`, which hands this pump a continuation.
struct AdminStreamPump: Sendable {
    /// Fully-built `ws(s)://…/admin/stream?db=&table=` target.
    let url: URL
    /// Fresh transport per connection attempt (production:
    /// `URLSessionWebSocketTransport` with the admin subprotocol).
    let factory: @Sendable () -> any WebSocketTransport
    /// Backoff sleeps (injectable so tests run instantly).
    let scheduler: any WScheduler
    let backoffBaseMs: UInt64
    let backoffMaxMs: UInt64

    func run(_ continuation: AsyncThrowingStream<AdminStreamFrame, Error>.Continuation) async {
        var backoff = backoffBaseMs
        var transport: any WebSocketTransport
        do {
            transport = try await connect()
        } catch {
            continuation.finish(throwing: Self.classifyConnect(error))
            return
        }
        while !Task.isCancelled {
            do {
                while !Task.isCancelled {
                    let text = try await transport.receive()
                    if let frame = AdminStreamFrame.decode(text) {
                        continuation.yield(frame)
                    }
                }
                break
            } catch {
                if Task.isCancelled {
                    break
                }
                if let close = error as? TransportCloseError, close.code == 4401 {
                    continuation.finish(throwing: RtDbError(
                        code: .unauthorized,
                        message: "admin credential no longer valid"
                    ))
                    return
                }
                // Transport loss or unexpected close: reconnect on backoff.
                // A failed reconnect keeps retrying — only cancellation or a
                // terminal 4401 ends the loop from here.
                await scheduler.sleep(backoff)
                backoff = min(backoff * 2, backoffMaxMs)
                do {
                    transport = try await connect()
                    backoff = backoffBaseMs
                } catch {
                    continue
                }
            }
        }
        await transport.close(code: 1000)
        continuation.finish()
    }

    /// Initial-connect failures finish the stream instead of retrying: a
    /// bad credential must not look like a hang. URLSession cannot surface
    /// the handshake's HTTP status (unlike the rust client's tungstenite,
    /// which classifies 401/403), so the rejection surfaces as INTERNAL with
    /// the underlying error unless the transport already raised an
    /// `RtDbError` (e.g. the ws/wss scheme guard).
    private static func classifyConnect(_ error: Error) -> RtDbError {
        if let rtDb = error as? RtDbError {
            return rtDb
        }
        return RtDbError(code: .internal, message: "admin stream connect failed: \(error)")
    }

    private func connect() async throws -> any WebSocketTransport {
        let transport = factory()
        try await transport.connect(to: url)
        return transport
    }
}

// MARK: - RtDbAdminClient + streamAdmin

let adminStreamBackoffBaseMs: UInt64 = 500
let adminStreamBackoffMaxMs: UInt64 = 15000

extension RtDbAdminClient {
    /// `WS /admin/stream?db=&table=` → the live op feed as an
    /// `AsyncThrowingStream` of frames — op events (a ≤200-event replay, then
    /// live) interleaved with ~1s gauge snapshots.
    ///
    /// `db`/`table` filter both the replay and the live stream, exactly as on
    /// `opsRecent` (both optional here; nil spans every database/table).
    /// Every (re)connection replays up to 200 ring events, so duplicates
    /// after a blip are expected — dedup on the `(feedEpoch, seq)` pair every
    /// `OpEvent` carries: track the max `seq` seen per `feedEpoch`, and read
    /// an epoch change as a counter reset (server restart) rather than
    /// dropped events. Transport drops and unexpected closes reconnect
    /// automatically on the exponential backoff; a mid-stream close 4401
    /// (credential revoked, SEC-006) finishes the stream with
    /// `ErrorCode.unauthorized` and is never retried; an initial-connect
    /// failure (a rejected upgrade, a bad URL) finishes the stream with an
    /// `RtDbError` instead of retrying.
    /// Break out of the loop or cancel the consuming task to close the
    /// socket. The admin key rides the `rtdb-admin.<token>` WebSocket
    /// subprotocol — URLSessionWebSocketTask cannot set arbitrary headers,
    /// and the server accepts the subprotocol (the browser path) as an
    /// alternative to the Bearer header.
    ///
    /// ```swift
    /// for try await frame in try await client.streamAdmin(db: "mydb") {
    ///     switch frame {
    ///     case .op(let event): print(event.db, event.kind, event.docId)
    ///     case .gauges(let gauges): print(gauges.queriesTotal)
    ///     }
    /// }
    /// ```
    public func streamAdmin(
        db: String? = nil, table: String? = nil
    ) -> AsyncThrowingStream<AdminStreamFrame, Error> {
        guard let url = Self.streamURL(baseUrl, db: db, table: table) else {
            return AsyncThrowingStream { continuation in
                continuation.finish(
                    throwing: RtDbError(code: .badRequest, message: "invalid server url: \(baseUrl)")
                )
            }
        }
        let key = adminKey
        let factory = streamTransportFactoryOverride ?? { _ in
            URLSessionWebSocketTransport(subprotocol: "rtdb-admin.\(key)")
        }
        let scheduler = streamSchedulerOverride ?? SystemScheduler()
        let pump = AdminStreamPump(
            url: url,
            factory: { factory(url) },
            scheduler: scheduler,
            backoffBaseMs: streamBackoffBaseMs,
            backoffMaxMs: streamBackoffMaxMs
        )
        return AsyncThrowingStream { continuation in
            let task = Task {
                await pump.run(continuation)
            }
            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    /// Flip http(s)→ws(s) and build `/admin/stream?db=&table=`; filters are
    /// percent-encoded by URLComponents, never interpolated.
    static func streamURL(_ base: String, db: String?, table: String?) -> URL? {
        guard var components = URLComponents(string: base) else { return nil }
        components.scheme = components.scheme?.lowercased() == "https" ? "wss" : "ws"
        components.path = "/admin/stream"
        var items: [URLQueryItem] = []
        if let db {
            items.append(URLQueryItem(name: "db", value: db))
        }
        if let table {
            items.append(URLQueryItem(name: "table", value: table))
        }
        components.queryItems = items.isEmpty ? nil : items
        return components.url
    }
}

extension RtDbAdminClient {
    /// Internal test seam: script transports and shrink backoff (AdminStreamTests).
    func setStreamOverrides(
        factory: (@Sendable (URL) -> any WebSocketTransport)?,
        backoffBaseMs: UInt64,
        backoffMaxMs: UInt64
    ) {
        streamTransportFactoryOverride = factory
        streamBackoffBaseMs = backoffBaseMs
        streamBackoffMaxMs = backoffMaxMs
    }
}
