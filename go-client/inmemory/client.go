// The user-facing in-memory client — the same usage surface as
// httpclient/wsclient (structural, no shared interface package): push a
// schema, query, mutate, subscribe, presence, and the deterministic Tick.
// Plan Task 25's interface block.
package inmemory

import (
	"context"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Client is the in-memory engine's user surface. Construct with New.
type Client struct {
	store          *Store
	presence       *PresenceRooms
	presenceUser   wire.AuthedUser
	connID         string
	joinedRooms    map[string]bool
	presenceUnsubs map[string]func()
}

// Options configures New. All fields optional.
type Options struct {
	// Now injects the epoch-millis clock for deterministic ids/timestamps.
	Now func() int64
	// Random injects the [0,1) RNG for deterministic id minting.
	Random func() float64
	// ConnectionID is this client's stable presence identity (a c{N} counter
	// when empty).
	ConnectionID string
	// PresenceUser is stamped on this client's presence entries.
	PresenceUser wire.AuthedUser
	// PresenceRooms shares a presence backing between clients; nil gets a
	// private instance.
	PresenceRooms *PresenceRooms
}

// New constructs a fresh engine. Defaults: system clock, constant 0.5 RNG,
// a private presence backing, and a nameless {kind: user} identity.
func New(opts Options) *Client {
	store := NewStore()
	if opts.Now != nil {
		store.WithClock(opts.Now)
	}
	if opts.Random != nil {
		store.WithRandom(opts.Random)
	}
	connID := opts.ConnectionID
	if connID == "" {
		connID = "c1"
	}
	rooms := opts.PresenceRooms
	if rooms == nil {
		rooms = NewPresenceRooms()
	}
	return &Client{
		store:          store,
		presence:       rooms,
		presenceUser:   opts.PresenceUser,
		connID:         connID,
		joinedRooms:    map[string]bool{},
		presenceUnsubs: map[string]func(){},
	}
}

// Store exposes the underlying engine state (test/inspection seam).
func (c *Client) Store() *Store { return c.store }

// PushSchema installs the schema (additive merges; destructive changes
// rejected). See Store.PushSchema.
func (c *Client) PushSchema(s wire.JSONValue) error { return c.store.PushSchema(s) }

// Query runs a one-shot query. ctx is accepted for surface parity; the
// in-memory engine is synchronous and does not observe it.
func (c *Client) Query(ctx context.Context, q dsl.TableQuery) (wire.JSONValue, error) {
	_ = ctx
	return EvalQuery(c.store, q.Build())
}

// Mutate executes a transaction with an optional idempotency key.
func (c *Client) Mutate(ctx context.Context, txn wire.Transaction, idem string) ([]wire.StepResult, error) {
	_ = ctx
	return ApplyTxn(c.store, txn, idem)
}

// ApplyMigration plans/applies a declarative migration against the engine.
func (c *Client) ApplyMigration(directives []admin.Directive, dryRun bool) (admin.MigrateResult, error) {
	return ApplyMigration(c.store, directives, dryRun)
}

// Tick advances the scheduler/TTL deterministically (test-only knob; the
// corpus runner never ticks). Returns the number of reaped docs.
func (c *Client) Tick(now int64) int { return c.store.Tick(now) }

// Subscribe returns a live subscription: the updates channel receives the
// initial snapshot synchronously at subscribe time, then one snapshot per
// real change to the query's result (the server's re-run-on-change
// semantics). A failing query never fires (the rust port's documented
// divergence). The channel is buffered; a subscriber that stops draining
// will stall notify fan-out — the harness is driven synchronously by tests,
// matching the rust/ts engines.
func (c *Client) Subscribe(q dsl.TableQuery) *wsSubscription {
	query := q.Build()
	table := query.Table
	sub := &Subscription{
		Query: query,
		Table: table,
		alive: &syncFlag{},
	}
	sub.alive.set(true)
	ws := &wsSubscription{
		sub:     sub,
		store:   c.store,
		updates: make(chan wire.JSONValue, 16),
	}
	sub.Callback = func(doc wire.JSONValue) {
		// notifySubs fires outside its iteration; a dead subscription's
		// send is skipped so Unsubscribe cannot race a concurrent fire.
		if !ws.closing.get() {
			ws.updates <- doc
		}
	}
	c.store.mu.Lock()
	if initial, err := evalQueryLocked(c.store, query); err == nil {
		sub.last = diffCanonical(initial, &query)
		sub.hasLast = true
		ws.updates <- initial
	}
	c.store.subscribers = append(c.store.subscribers, sub)
	c.store.mu.Unlock()
	return ws
}

// wsSubscription is the handle returned by Subscribe.
type wsSubscription struct {
	sub     *Subscription
	store   *Store
	updates chan wire.JSONValue
	closing syncFlag
	once    syncFlag
}

// Updates is the snapshot channel.
func (s *wsSubscription) Updates() <-chan wire.JSONValue { return s.updates }

// Unsubscribe detaches the listener and closes the updates channel.
func (s *wsSubscription) Unsubscribe() {
	s.closing.set(true)
	s.sub.alive.set(false)
	s.store.mu.Lock()
	live := s.store.subscribers[:0]
	for _, sub := range s.store.subscribers {
		if sub.alive.get() {
			live = append(live, sub)
		}
	}
	s.store.subscribers = live
	s.store.mu.Unlock()
	if !s.once.swap(true) {
		close(s.updates)
	}
}

// JoinPresence joins room with an initial state and registers this client's
// room listener; the callback fires with the current member list on join and
// again on every local change (own update, or a peer's on a shared backing).
func (c *Client) JoinPresence(room string, state wire.JSONValue, onUpdate func([]wire.PresenceMember)) {
	c.presence.Join(room, wire.PresenceMember{
		ConnectionID: c.connID,
		User:         c.presenceUser,
		State:        state,
	})
	if onUpdate != nil {
		c.presenceUnsubs[room] = c.presence.Subscribe(room, onUpdate)
		onUpdate(c.presence.Snapshot(room))
	}
	c.joinedRooms[room] = true
}

// UpdatePresence broadcasts updated state for this connection in room (no-op
// for a non-member); ttlMs > 0 schedules a state-nulling expiry.
func (c *Client) UpdatePresence(room string, state wire.JSONValue, ttlMs *uint64) {
	c.presence.Update(room, c.connID, state, ttlMs, c.store.Now())
}

// LeavePresence leaves the room and drops this client's listener.
func (c *Client) LeavePresence(room string) {
	if unsub, ok := c.presenceUnsubs[room]; ok {
		unsub()
		delete(c.presenceUnsubs, room)
	}
	c.presence.Leave(room, c.connID)
	delete(c.joinedRooms, room)
}

// PresenceSnapshot returns the room's members in join order.
func (c *Client) PresenceSnapshot(room string) []wire.PresenceMember {
	return c.presence.Snapshot(room)
}
