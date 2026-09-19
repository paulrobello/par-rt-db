// go-client/wsclient/client.go
package wsclient

// Mirrors rust-client/src/ws.rs structure: a manager goroutine owns the
// connection — auth handshake, heartbeat, exponential-backoff reconnect
// with full replay (re-auth + re-subscribe every live query), and frame
// dispatch to registered handlers.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// TokenProvider supplies the bearer token; called on every (re)connect so
// refresh-capable credentials work.
type TokenProvider func(ctx context.Context) (string, error)

// Client is the /sync websocket client.
type Client struct {
	url       string
	db        string
	tokens    TokenProvider
	backoff   reconnectBackoff
	heartbeat time.Duration

	mu      sync.Mutex
	conn    transport
	authErr error
	closed  bool

	// replayed on every reconnect: active queries by canonical key.
	subsMu sync.Mutex
	subs   map[string]*Subscription
	// replies waits by correlation id; guarded by subsMu (same lock space,
	// correlation ids never collide with query ids).
	replies map[string]*pendingReply

	handlerMu sync.Mutex
	onServer  func(wire.ServerMessage) // test/observer hook

	wg sync.WaitGroup
}

type reconnectBackoff struct {
	base time.Duration
	max  time.Duration
}

// Option configures the client.
type Option func(*clientConfig)

type clientConfig struct {
	backoffBase time.Duration
	backoffMax  time.Duration
	heartbeat   time.Duration
}

func (c *clientConfig) apply(opts []Option) *clientConfig {
	c.backoffBase = 500 * time.Millisecond
	c.backoffMax = 15 * time.Second
	c.heartbeat = 20 * time.Second
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithBackoff overrides reconnect backoff (defaults 500ms→15s).
func WithBackoff(base, max time.Duration) Option {
	return func(c *clientConfig) { c.backoffBase, c.backoffMax = base, max }
}

// WithHeartbeat overrides the ping cadence (default 20s).
func WithHeartbeat(d time.Duration) Option {
	return func(c *clientConfig) { c.heartbeat = d }
}

// NewClient builds a ws client; Connect must be called before use.
func NewClient(wsURL, db string, tokens TokenProvider, opts ...Option) *Client {
	cfg := (&clientConfig{}).apply(opts)
	return &Client{
		url:       wsURL,
		db:        db,
		tokens:    tokens,
		backoff:   reconnectBackoff{base: cfg.backoffBase, max: cfg.backoffMax},
		heartbeat: cfg.heartbeat,
		subs:      map[string]*Subscription{},
	}
}

// Connect dials, authenticates, and starts the manager goroutine.
func (c *Client) Connect(ctx context.Context) error {
	tok, err := c.tokens(ctx)
	if err != nil {
		return fmt.Errorf("wsclient: token: %w", err)
	}
	tr, err := dial(ctx, c.url)
	if err != nil {
		return fmt.Errorf("wsclient: dial: %w", err)
	}
	if err := c.authenticate(ctx, tr, tok); err != nil {
		tr.Close()
		return err
	}
	c.mu.Lock()
	c.conn = tr
	c.authErr = nil
	c.mu.Unlock()
	c.wg.Add(1)
	go c.manager(context.WithoutCancel(ctx))
	return nil
}

// authenticate sends auth{token, db, protocolVersion} and waits for
// authOk/authErr on the given transport (used at connect and replay).
func (c *Client) authenticate(ctx context.Context, tr transport, token string) error {
	tok := token
	frame, err := json.Marshal(wire.ClientAuth{
		Token:           &tok,
		DB:              c.db,
		ProtocolVersion: wirePtr(),
	})
	if err != nil {
		return err
	}
	if err := tr.Write(ctx, frame); err != nil {
		return fmt.Errorf("wsclient: auth write: %w", err)
	}
	for {
		_, data, err := tr.Read(ctx)
		if err != nil {
			return fmt.Errorf("wsclient: auth read: %w", err)
		}
		msg, err := wire.UnmarshalServerMessage(data)
		if err != nil {
			return fmt.Errorf("wsclient: auth frame: %w", err)
		}
		switch m := msg.(type) {
		case wire.ServerAuthOk:
			return nil
		case wire.ServerAuthErr:
			return &AuthError{Envelope: m.Error}
		default:
			// pre-auth frames are protocol violations; keep waiting is unsafe
			return fmt.Errorf("wsclient: unexpected frame before authOk")
		}
	}
}

func wirePtr() *uint32 {
	v := uint32(wire.PROTOCOL_VERSION)
	return &v
}

// manager drives the read pump; on connection loss it reconnects with
// exponential backoff and replays auth + all live subscriptions.
func (c *Client) manager(ctx context.Context) {
	defer c.wg.Done()
	delay := c.backoff.base
	for {
		tr := c.current()
		if tr == nil {
			return
		}
		err := c.readPump(ctx, tr)
		if c.isClosed() || ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		_ = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay *= 2
		if delay > c.backoff.max {
			delay = c.backoff.max
		}
		if err := c.reconnect(ctx); err != nil {
			continue
		}
		delay = c.backoff.base
	}
}

// reconnect redials + re-auths + replays subscriptions.
func (c *Client) reconnect(ctx context.Context) error {
	tok, err := c.tokens(ctx)
	if err != nil {
		return err
	}
	tr, err := dial(ctx, c.url)
	if err != nil {
		return err
	}
	if err := c.authenticate(ctx, tr, tok); err != nil {
		tr.Close()
		return err
	}
	c.mu.Lock()
	c.conn = tr
	c.mu.Unlock()
	// replay every live subscription on the fresh connection
	c.subsMu.Lock()
	live := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		live = append(live, s)
	}
	c.subsMu.Unlock()
	for _, s := range live {
		s.resubscribe(ctx, tr)
	}
	return nil
}

func (c *Client) readPump(ctx context.Context, tr transport) error {
	for {
		_, data, err := tr.Read(ctx)
		if err != nil {
			return err
		}
		msg, err := wire.UnmarshalServerMessage(data)
		if err != nil {
			continue // malformed frame: drop, keep the session
		}
		c.dispatch(ctx, msg)
	}
}

func (c *Client) dispatch(ctx context.Context, msg wire.ServerMessage) {
	c.handlerMu.Lock()
	hook := c.onServer
	c.handlerMu.Unlock()
	if hook != nil {
		hook(msg)
	}
	c.routeReply(msg)
	switch m := msg.(type) {
	case wire.ServerQueryUpdate:
		c.subsMu.Lock()
		var targets []*Subscription
		for _, s := range c.subs {
			if s.queryID == m.QueryID {
				targets = append(targets, s)
			}
		}
		c.subsMu.Unlock()
		for _, s := range targets {
			s.deliver(Snapshot{Kind: SnapshotValue, Value: m.Result})
		}
	case wire.ServerSubscribeErr:
		c.subsMu.Lock()
		s := c.subs[m.QueryID]
		c.subsMu.Unlock()
		if s != nil {
			s.deliver(Snapshot{Kind: SnapshotError, Err: envelopeToRtDb(m.Error)})
		}
	case wire.ServerPong:
		// liveness proof; the heartbeat writer treats any successful read
		// loop iteration as healthy.
	default:
		// mutate/schedule/presence replies surface via their own surfaces.
	}
}

func (c *Client) current() transport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Close drains the manager and closes the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	tr := c.conn
	c.conn = nil
	c.mu.Unlock()
	if tr != nil {
		_ = tr.Close()
	}
	c.wg.Wait()
	return nil
}

// sendFrame writes one client frame on the live connection.
func (c *Client) sendFrame(ctx context.Context, data []byte) error {
	tr := c.current()
	if tr == nil {
		return errors.New("wsclient: not connected")
	}
	return tr.Write(ctx, data)
}

// setServerObserver installs a test/observer hook for every server frame.
func (c *Client) setServerObserver(fn func(wire.ServerMessage)) {
	c.handlerMu.Lock()
	c.onServer = fn
	c.handlerMu.Unlock()
}
