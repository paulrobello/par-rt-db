import Foundation

/// One delivered batch of change-feed ops (`feedEvents` after dedupe): ops are
/// oldest-first and strictly newer than anything previously yielded for the
/// same `feedEpoch`.
public struct ChangeFeedBatch: Equatable, Sendable {
    /// The change log's identity (the HTTP feed's `logId`).
    public var feedEpoch: String
    public var ops: [ChangeOp]
    /// The log head when the server built the frame.
    public var head: Int64

    public init(feedEpoch: String, ops: [ChangeOp], head: Int64) {
        self.feedEpoch = feedEpoch
        self.ops = ops
        self.head = head
    }
}

/// Per-feed client state held by `RtDbClient`: the resume cursor
/// `(epoch, lastSeq)` and the consumer's stream.
struct ChangeFeedState {
    let tables: [String]
    var epoch: String?
    var lastSeq: Int64?
    let continuation: AsyncThrowingStream<ChangeFeedBatch, Error>.Continuation
}

extension RtDbClient {
    /// Stream durable change-feed batches over the WebSocket (`feedSubscribe`).
    ///
    /// `since` omitted starts at the current log head; `since: N` streams ops
    /// with `seq > N`. `feedEpoch` (a previous batch's epoch) makes the server
    /// reject a replaced database. On every reconnect the client re-subscribes
    /// from the last yielded seq, drops ops it already yielded (at-least-once
    /// delivery, dedupe key `(feedEpoch, seq)`), and acks each batch.
    ///
    /// A `feedErr` finishes the stream by throwing the server's `RtDbError`
    /// with no retry; `CURSOR_EXPIRED` means resync over the HTTP changes path.
    /// Cancelling or dropping the stream sends `feedUnsubscribe`.
    public func watchChanges(
        tables: [String] = [], since: Int64? = nil, feedEpoch: String? = nil
    ) async -> AsyncThrowingStream<ChangeFeedBatch, Error> {
        let (stream, continuation) = AsyncThrowingStream<ChangeFeedBatch, Error>.makeStream(
            of: ChangeFeedBatch.self, throwing: Error.self, bufferingPolicy: .unbounded
        )
        let feedId = "feed-\(UUID().uuidString)"
        feedStates[feedId] = ChangeFeedState(
            tables: tables, epoch: feedEpoch, lastSeq: since, continuation: continuation
        )
        continuation.onTermination = { [weak self] _ in
            Task { await self?.cancelFeed(feedId) }
        }
        if let transport = currentTransport, state == .connected {
            _ = await sendFeedSubscribe(feedId, on: transport)
        }
        return stream
    }

    private func sendFeedSubscribe(_ feedId: String, on transport: any WebSocketTransport) async -> Bool {
        guard let feed = feedStates[feedId] else { return true }
        let message = ClientMessage.feedSubscribe(
            feedId: feedId, since: feed.lastSeq, feedEpoch: feed.epoch, tables: feed.tables
        )
        do {
            try await transport.send(Self.frame(message))
            return true
        } catch {
            return false
        }
    }

    /// Re-send `feedSubscribe` for every live feed with its resume cursor (the
    /// server dropped this connection's feeds on disconnect). False when a send
    /// failed, so the caller reconnects.
    func replayFeeds(on transport: any WebSocketTransport) async -> Bool {
        for feedId in Array(feedStates.keys) {
            guard await sendFeedSubscribe(feedId, on: transport) else { return false }
        }
        return true
    }

    func onFeedEvents(feedId: String, epoch: String, ops: [ChangeOp], head: Int64) async {
        guard var feed = feedStates[feedId] else { return }
        if let known = feed.epoch, known != epoch {
            feedStates[feedId] = nil
            feed.continuation.finish(throwing: RtDbError(
                code: .cursorExpired,
                message: "feedEpoch \(epoch) does not match \(known) — the database was replaced; resync"
            ))
            return
        }
        feed.epoch = epoch
        let floor = feed.lastSeq ?? Int64.min
        let fresh = ops.filter { $0.seq > floor }
        if let top = fresh.map(\.seq).max() {
            feed.lastSeq = top
        } else if ops.isEmpty, feed.lastSeq == nil {
            feed.lastSeq = head
        }
        feedStates[feedId] = feed
        if !fresh.isEmpty {
            feed.continuation.yield(ChangeFeedBatch(feedEpoch: epoch, ops: fresh, head: head))
        }
        if let acked = ops.map(\.seq).max(), let transport = currentTransport, state == .connected {
            try? await transport.send(Self.frame(.feedAck(feedId: feedId, seq: acked)))
        }
    }

    func onFeedErr(feedId: String, error: RtDbError) {
        feedStates.removeValue(forKey: feedId)?.continuation.finish(throwing: error)
    }

    private func cancelFeed(_ feedId: String) async {
        guard feedStates.removeValue(forKey: feedId) != nil else { return }
        if let transport = currentTransport, state == .connected {
            try? await transport.send(Self.frame(.feedUnsubscribe(feedId: feedId)))
        }
    }

    func finishAllFeeds() {
        for feed in feedStates.values {
            feed.continuation.finish()
        }
        feedStates.removeAll()
    }
}
