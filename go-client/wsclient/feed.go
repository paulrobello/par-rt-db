// go-client/wsclient/feed.go
package wsclient

// WebSocket change-feed push channel (feedSubscribe / feedEvents / feedAck /
// feedErr). The server streams the durable change log oldest-first with
// at-least-once delivery; this side owns the cursor. It tracks the last
// received (feedEpoch, seq), re-sends feedSubscribe{since, feedEpoch} on
// every reconnect, drops replayed ops (seq <= cursor within an epoch), and
// acks a batch once the consumer has taken it off the channel. A feedErr is
// terminal and never retried: CURSOR_EXPIRED means resync over the HTTP
// changes path.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// FeedOptions configures WatchChanges. The zero value streams every table
// from the current head.
type FeedOptions struct {
	// Tables restricts the feed to these tables (empty = all permitted).
	Tables []string
	// Since resumes after this seq (nil = start at the current head; a
	// pointer so 0 is distinguishable from absent).
	Since *int64
	// FeedEpoch pins the log generation Since belongs to (the HTTP feed's
	// logId). Empty = learn it from the first frame.
	FeedEpoch string
}

// FeedBatch is one deduplicated, in-order group of ops. Ops is never empty.
type FeedBatch struct {
	Ops   []wire.ChangeOp
	Epoch string
	Head  int64
}

// Feed is a live change-feed handle. Read batches from Batches(); the
// channel closes when the feed ends, after which Err reports why.
type Feed struct {
	id     string
	client *Client
	tables []string
	out    chan FeedBatch
	wake   chan struct{}
	done   chan struct{} // closed by Close
	fin    chan struct{} // closed when the feed ends for any reason

	mu        sync.Mutex
	epoch     string
	cursor    int64
	hasCursor bool
	queue     []queuedBatch
	terminal  bool
	err       error
	closed    bool
}

type queuedBatch struct {
	batch FeedBatch
	max   int64
}

// feedChanCap bounds the batch channel; the server's unacked window bounds
// the internal queue.
const feedChanCap = 16

// WatchChanges opens a change-feed push channel. Cancelling ctx or calling
// Close sends feedUnsubscribe and closes the batch channel.
func (c *Client) WatchChanges(ctx context.Context, opts FeedOptions) (*Feed, error) {
	f := &Feed{
		id:     newID(),
		client: c,
		tables: append([]string(nil), opts.Tables...),
		out:    make(chan FeedBatch, feedChanCap),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
		fin:    make(chan struct{}),
		epoch:  opts.FeedEpoch,
	}
	if opts.Since != nil {
		f.cursor, f.hasCursor = *opts.Since, true
	}
	frame, err := f.subscribeFrame()
	if err != nil {
		return nil, err
	}
	c.feedsMu.Lock()
	if c.feeds == nil {
		c.feeds = map[string]*Feed{}
	}
	c.feeds[f.id] = f
	c.feedsMu.Unlock()
	if err := c.sendFrame(ctx, frame); err != nil {
		c.removeFeed(f.id)
		return nil, err
	}
	go f.pump()
	go func() {
		select {
		case <-ctx.Done():
			f.Close()
		case <-f.fin:
		}
	}()
	return f, nil
}

// Batches yields deduplicated batches in seq order until the feed ends.
func (f *Feed) Batches() <-chan FeedBatch { return f.out }

// Err reports why the feed ended: nil after a clean Close, otherwise the
// terminal error (an *errors.RtDbError for a server feedErr, e.g.
// CURSOR_EXPIRED). Valid once Batches is closed.
func (f *Feed) Err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// Close ends the feed and sends feedUnsubscribe. Safe to call repeatedly.
func (f *Feed) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	terminal := f.terminal
	f.mu.Unlock()
	close(f.done)
	if !terminal {
		close(f.fin)
	}
	f.client.removeFeed(f.id)
	if terminal {
		return
	}
	if frame, err := json.Marshal(wire.ClientFeedUnsubscribe{FeedID: f.id}); err == nil {
		_ = f.client.sendFrame(context.Background(), frame)
	}
}

// subscribeFrame builds the feedSubscribe frame from the current cursor.
func (f *Feed) subscribeFrame() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	req := wire.ClientFeedSubscribe{FeedID: f.id, Tables: f.tables}
	if f.hasCursor {
		since := f.cursor
		req.Since = &since
	}
	if f.epoch != "" {
		epoch := f.epoch
		req.FeedEpoch = &epoch
	}
	return json.Marshal(req)
}

// resubscribe re-sends feedSubscribe with the last received cursor on a
// replayed connection.
func (f *Feed) resubscribe(ctx context.Context, tr transport) {
	frame, err := f.subscribeFrame()
	if err != nil {
		return
	}
	f.client.writeMu.Lock()
	defer f.client.writeMu.Unlock()
	_ = tr.Write(ctx, frame)
}

// onEvents folds one feedEvents frame: epoch check, replay dedupe, enqueue.
func (f *Feed) onEvents(m wire.ServerFeedEvents) {
	f.mu.Lock()
	if f.terminal || f.closed {
		f.mu.Unlock()
		return
	}
	if f.epoch != "" && m.FeedEpoch != f.epoch {
		f.mu.Unlock()
		f.fail(rtdberrors.New(rtdberrors.CodeCursorExpired,
			"feedEpoch changed ("+f.epoch+" -> "+m.FeedEpoch+"): the database was replaced; resync"))
		f.sendUnsubscribe()
		return
	}
	f.epoch = m.FeedEpoch
	ops := make([]wire.ChangeOp, 0, len(m.Ops))
	for _, op := range m.Ops {
		if f.hasCursor && op.Seq <= f.cursor {
			continue
		}
		ops = append(ops, op)
		f.cursor, f.hasCursor = op.Seq, true
	}
	if len(m.Ops) == 0 && !f.hasCursor {
		// Confirming frame with no cursor yet: head is a safe resume point.
		f.cursor, f.hasCursor = m.Head, true
	}
	if len(ops) > 0 {
		f.queue = append(f.queue, queuedBatch{
			batch: FeedBatch{Ops: ops, Epoch: m.FeedEpoch, Head: m.Head},
			max:   ops[len(ops)-1].Seq,
		})
	}
	f.mu.Unlock()
	f.signal()
}

// fail ends the feed with err after already-queued batches drain.
func (f *Feed) fail(err error) {
	f.mu.Lock()
	if f.terminal || f.closed {
		f.mu.Unlock()
		return
	}
	f.terminal, f.err = true, err
	f.mu.Unlock()
	close(f.fin)
	f.client.removeFeed(f.id)
	f.signal()
}

func (f *Feed) sendUnsubscribe() {
	if frame, err := json.Marshal(wire.ClientFeedUnsubscribe{FeedID: f.id}); err == nil {
		_ = f.client.sendFrame(context.Background(), frame)
	}
}

func (f *Feed) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// pump moves queued batches to the consumer channel, acking each after the
// hand-off, and closes the channel on Close or after a terminal error drains.
func (f *Feed) pump() {
	defer close(f.out)
	for {
		f.mu.Lock()
		if len(f.queue) == 0 {
			terminal := f.terminal
			f.mu.Unlock()
			if terminal {
				return
			}
			select {
			case <-f.wake:
				continue
			case <-f.done:
				return
			}
		}
		qb := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		select {
		case f.out <- qb.batch:
		case <-f.done:
			return
		}
		if frame, err := json.Marshal(wire.ClientFeedAck{FeedID: f.id, Seq: qb.max}); err == nil {
			_ = f.client.sendFrame(context.Background(), frame)
		}
	}
}

func (c *Client) removeFeed(id string) {
	c.feedsMu.Lock()
	delete(c.feeds, id)
	c.feedsMu.Unlock()
}

// routeFeed delivers a feed frame to its handle (unknown ids are ignored).
func (c *Client) routeFeed(msg wire.ServerMessage) {
	var id string
	switch m := msg.(type) {
	case wire.ServerFeedEvents:
		id = m.FeedID
	case wire.ServerFeedErr:
		id = m.FeedID
	default:
		return
	}
	c.feedsMu.Lock()
	f := c.feeds[id]
	c.feedsMu.Unlock()
	if f == nil {
		return
	}
	switch m := msg.(type) {
	case wire.ServerFeedEvents:
		f.onEvents(m)
	case wire.ServerFeedErr:
		f.fail(envelopeToRtDb(m.Error))
	}
}

// replayFeeds re-sends feedSubscribe for every live feed.
func (c *Client) replayFeeds(ctx context.Context, tr transport) {
	c.feedsMu.Lock()
	live := make([]*Feed, 0, len(c.feeds))
	for _, f := range c.feeds {
		live = append(live, f)
	}
	c.feedsMu.Unlock()
	for _, f := range live {
		f.resubscribe(ctx, tr)
	}
}

// failFeeds terminates every live feed (client shutdown).
func (c *Client) failFeeds() {
	c.feedsMu.Lock()
	live := make([]*Feed, 0, len(c.feeds))
	for _, f := range c.feeds {
		live = append(live, f)
	}
	c.feedsMu.Unlock()
	for _, f := range live {
		f.fail(errors.New("wsclient: client is closed"))
	}
}
