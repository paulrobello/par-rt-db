// go-client/wsclient/subscription.go
package wsclient

// Mirrors rust ws.rs subscription semantics: channel-based snapshots
// (Pending → Value/Error), dedupe by canonical query shape, refcounted
// Close that sends unsubscribe on the last handle.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// SnapshotKind mirrors rust's Pending | Value | Error.
type SnapshotKind uint8

// Snapshot kinds.
const (
	SnapshotPending SnapshotKind = iota
	SnapshotValue
	SnapshotError
)

// Snapshot is one subscription update.
type Snapshot struct {
	Kind  SnapshotKind
	Value wire.JSONValue
	Err   *rtdberrors.RtDbError
}

// AuthError reports a rejected auth handshake.
type AuthError struct {
	Envelope wire.ErrorEnvelope
}

// Error implements error.
func (e *AuthError) Error() string {
	return fmt.Sprintf("wsclient: auth rejected: %s: %s", e.Envelope.Code, e.Envelope.Message)
}

// Subscription is one live query.
type Subscription struct {
	queryID string
	key     string // canonical query key for dedupe
	client  *Client
	q       wire.Query

	mu      sync.Mutex
	refs    int
	closed  bool
	updates chan Snapshot
}

// updatesChanCap bounds the snapshot buffer.
const updatesChanCap = 64

// Subscribe registers a live query; the first Pending snapshot is
// delivered immediately. Identical queries (canonical JSON equality) share
// ONE server-side subscription; each Subscribe acquires one handle of the
// shared refcount.
func (c *Client) Subscribe(ctx context.Context, q wire.Query) (*Subscription, error) {
	key, err := canonicalKey(q)
	if err != nil {
		return nil, err
	}
	// One critical section across the dedupe check AND registration: two
	// concurrent identical Subscribes must not both miss the check and
	// double-register (the second registration would overwrite byKey, so the
	// first handle's Close would delete the second's dedupe entry).
	c.subsMu.Lock()
	if existing := c.byKey[key]; existing != nil {
		existing.mu.Lock()
		existing.refs++
		existing.mu.Unlock()
		c.subsMu.Unlock()
		return existing, nil
	}
	id := newID()
	sub := &Subscription{
		queryID: id,
		key:     key,
		client:  c,
		q:       q,
		refs:    1,
		updates: make(chan Snapshot, updatesChanCap),
	}
	sub.deliver(Snapshot{Kind: SnapshotPending})
	// register before sending so an immediate queryUpdate is not dropped (M5)
	c.subs[id] = sub
	c.byKey[key] = sub
	c.subsMu.Unlock()
	frame, err := json.Marshal(wire.ClientSubscribe{QueryID: id, Query: q})
	if err != nil {
		return nil, err
	}
	if err := c.sendFrame(ctx, frame); err != nil {
		return nil, err
	}
	return sub, nil
}

// Updates yields snapshots until the subscription closes.
func (s *Subscription) Updates() <-chan Snapshot {
	return s.updates
}

// Close drops one handle; the last one unsubscribes server-side.
func (s *Subscription) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.refs--
	if s.refs > 0 {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.updates)
	s.mu.Unlock()
	frame, err := json.Marshal(wire.ClientUnsubscribe{QueryID: s.queryID})
	if err == nil {
		_ = s.client.sendFrame(context.Background(), frame)
	}
	s.client.subsMu.Lock()
	delete(s.client.subs, s.queryID)
	delete(s.client.byKey, s.key)
	s.client.subsMu.Unlock()
}

// deliver pushes a snapshot without blocking the read pump. The send (and
// the closed check) happen under s.mu so a concurrent Close can never race
// a send on the closed channel (C3): Close closes under the same lock.
func (s *Subscription) deliver(snap Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.updates <- snap:
	default:
		// buffer full: drop the oldest and enqueue
		select {
		case <-s.updates:
		default:
		}
		select {
		case s.updates <- snap:
		default:
		}
	}
}

// resubscribe re-sends the subscribe frame on a replayed connection.
func (s *Subscription) resubscribe(ctx context.Context, tr transport) {
	frame, err := json.Marshal(wire.ClientSubscribe{QueryID: s.queryID, Query: s.query()})
	if err != nil {
		return
	}
	_ = tr.Write(ctx, frame)
}

// query returns the stored wire query (kept for replay).
func (s *Subscription) query() wire.Query {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q
}

// canonicalKey marshals the query — encoding/json sorts map keys, so the
// marshal output IS the canonical form for dedupe.
func canonicalKey(q wire.Query) (string, error) {
	b, err := json.Marshal(q)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// newID mints a 128-bit hex id; if the crypto RNG fails (cannot on normal
// platforms), a time-and-counter fallback keeps ids unique within a run.
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), idFallback.Add(1))
}

var idFallback atomic.Uint64
