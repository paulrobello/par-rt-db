// go-client/admin/stream.go
package wsclient

// Mirrors rust-client/src/admin/stream.rs and ts-client's streamAdmin():
// the realtime op-feed WebSocket /admin/stream. Machine client — the admin
// key rides the Authorization: Bearer header (the server prefers it; the
// rtdb-admin.<token> subprotocol is an alternative the server accepts, the
// one browsers must use). A frame whose kind is unknown is skipped, not
// fatal, matching ts-client's parseAdminStreamFrame and the rust
// AdminStream::next loop. Every reconnect replays up to 200 ring events, so
// duplicates after a blip are expected — OpEvent carries no sequence to
// dedup on.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/paulrobello/par-rt-db/go-client/admin"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
)

// AdminStreamFrame mirrors ts-client's AdminStreamFrame union: a document
// op event (replay then live), or a ~1s server metrics snapshot. Payloads
// decode through the existing OpEvent / MetricsSnapshot types.
type AdminStreamFrame struct {
	Kind   string                 `json:"kind"`
	Event  *admin.OpEvent         `json:"event,omitempty"`
	Gauges *admin.MetricsSnapshot `json:"gauges,omitempty"`
}

// AdminStreamEvent is one channel delivery from StreamAdmin: exactly one of
// Frame or Err is non-nil. Err is terminal — the stream ends after it
// (close 4401: the credential stopped validating mid-stream, SEC-006).
type AdminStreamEvent struct {
	Frame *AdminStreamFrame
	Err   error
}

// streamBackoffBase/Max mirror the wsclient's reconnect schedule
// (500ms→15s doubling). Vars so tests can shorten the waits.
var (
	streamBackoffBase = 500 * time.Millisecond
	streamBackoffMax  = 15 * time.Second
)

// dialStatusRe extracts a rejected-handshake status from coder/websocket
// v1.8.15's dial error ("expected handshake response status code 101 but
// got 401"). The library exposes no typed accessor, so the message is the
// only seam; revisit on a dependency bump.
var dialStatusRe = regexp.MustCompile(`but got (\d+)`)

// AdminStreamClient drives the /admin/stream op-feed WebSocket for a caller
// holding the instance admin key. It lives in wsclient rather than admin so
// importing the admin package never transitively pulls the websocket
// dependency — the stdlib-only rule guard_test.go enforces for the root,
// wire, dsl, errors, httpclient, and inmemory packages.
type AdminStreamClient struct {
	baseURL  string
	adminKey string
}

// NewAdminStreamClient builds a stream client against baseURL, carrying the
// instance admin key as the bearer on every (re)connect.
func NewAdminStreamClient(baseURL, adminKey string) *AdminStreamClient {
	return &AdminStreamClient{baseURL: baseURL, adminKey: adminKey}
}

// StreamAdmin opens /admin/stream?db=&table= and streams the op feed on a
// channel. db/table are optional (nil = all dbs / all tables) and filter
// both the replay and the live broadcast, exactly as on
// GET /admin/ops/recent.
//
// Terminal errors arrive as AdminStreamEvent{Err} and end the stream: a
// close 4401 (credential revoked mid-stream) is UNAUTHORIZED and is NOT
// retried — a revoked key is not a transient blip. Transport loss and
// unexpected closes reconnect on the wsclient's exponential backoff and
// continue streaming (every reconnect replays up to 200 ring events, so
// duplicates after a blip are expected). Cancel ctx to stop; the channel
// closes when the stream ends. A rejected initial handshake (401/403 or
// another non-101 status) is returned from this call itself as the client
// error type, classified by status like rust upgrade_error: 401 →
// UNAUTHORIZED, 403 → FORBIDDEN, else INTERNAL.
func (c *AdminStreamClient) StreamAdmin(ctx context.Context, db, table *string) (<-chan AdminStreamEvent, error) {
	wsURL, err := c.streamURL(db, table)
	if err != nil {
		return nil, err
	}
	tr, err := adminDial(ctx, wsURL, c.adminKey)
	if err != nil {
		return nil, err
	}
	ch := make(chan AdminStreamEvent)
	go func() {
		defer close(ch)
		backoff := streamBackoffBase
		for {
			err := streamLoop(ctx, tr, ch)
			if ctx.Err() != nil {
				return
			}
			if websocket.CloseStatus(err) == 4401 {
				sendEvent(ctx, ch, AdminStreamEvent{Err: &rtdberrors.RtDbError{
					Code:    rtdberrors.CodeUnauthorized,
					Message: closeReason(err),
				}})
				return
			}
			// Transport loss or unexpected close: reconnect on backoff.
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff *= 2
			if backoff > streamBackoffMax {
				backoff = streamBackoffMax
			}
			tr, err = adminDial(ctx, wsURL, c.adminKey)
			if err != nil {
				if status := dialStatus(err); status == 401 || status == 403 {
					sendEvent(ctx, ch, AdminStreamEvent{Err: &rtdberrors.RtDbError{
						Code:    rtdberrors.CodeUnauthorized,
						Message: "admin stream upgrade rejected with status " + strconv.Itoa(status),
					}})
					return
				}
				continue
			}
			backoff = streamBackoffBase
		}
	}()
	return ch, nil
}

// streamLoop reads frames until an error; unknown-kind, malformed, and
// unparseable frames are skipped (never fatal), and sends respect ctx so a
// consumer that stopped reading cannot leak the goroutine.
func streamLoop(ctx context.Context, tr *websocket.Conn, ch chan<- AdminStreamEvent) error {
	for {
		_, data, err := tr.Read(ctx)
		if err != nil {
			return err
		}
		if frame := parseAdminStreamFrame(data); frame != nil {
			if !sendEvent(ctx, ch, AdminStreamEvent{Frame: frame}) {
				return ctx.Err()
			}
		}
	}
}

// adminDial upgrades the URL with the bearer header, mapping a rejected
// handshake to the client error type by status: 401 → UNAUTHORIZED,
// 403 → FORBIDDEN, else INTERNAL (mirrors rust upgrade_error, whose
// body-envelope decode needs the handshake body — coder/websocket keeps
// only the status, so classification never depends on it).
func adminDial(ctx context.Context, wsURL, token string) (*websocket.Conn, error) {
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+token)
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		if status := dialStatus(err); status == 401 || status == 403 {
			code := rtdberrors.CodeUnauthorized
			if status == 403 {
				code = rtdberrors.CodeForbidden
			}
			return nil, &rtdberrors.RtDbError{
				Code:    code,
				Message: "admin stream upgrade rejected with status " + strconv.Itoa(status),
			}
		}
		return nil, fmt.Errorf("admin stream connect failed: %w", err)
	}
	return conn, nil
}

// streamURL builds ws(s)://host/admin/stream?db=&table= from the base URL,
// percent-encoding the filters (url.Values.Encode), never interpolating.
func (c *AdminStreamClient) streamURL(db, table *string) (string, error) {
	u, err := url.Parse(strings.TrimRight(c.baseURL, "/") + "/admin/stream")
	if err != nil {
		return "", fmt.Errorf("invalid server url: %w", err)
	}
	q := u.Query()
	if db != nil {
		q.Set("db", *db)
	}
	if table != nil {
		q.Set("table", *table)
	}
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	return u.String(), nil
}

// parseAdminStreamFrame mirrors ts-client's parseAdminStreamFrame and the
// rust union decode: unknown kind, missing payload, or malformed JSON is
// skipped (nil), never fatal. A known-kind frame with an invalid payload is
// also skipped — the stricter of the two references.
func parseAdminStreamFrame(data []byte) *AdminStreamFrame {
	var probe struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &probe); err != nil || probe.Kind == "" {
		return nil
	}
	var f AdminStreamFrame
	switch probe.Kind {
	case "op":
		if json.Unmarshal(data, &f) != nil || f.Event == nil {
			return nil
		}
	case "gauges":
		if json.Unmarshal(data, &f) != nil || f.Gauges == nil {
			return nil
		}
	default:
		return nil
	}
	return &f
}

// dialStatus extracts the rejected-handshake status from a dial error; 0
// when the error carries no status (connection refused, timeout, ...).
func dialStatus(err error) int {
	m := dialStatusRe.FindStringSubmatch(err.Error())
	if m == nil {
		return 0
	}
	n, convErr := strconv.Atoi(m[1])
	if convErr != nil {
		return 0
	}
	return n
}

// closeReason mirrors the rust client: the close frame's reason when
// present, else the default credential message.
func closeReason(err error) string {
	var ce websocket.CloseError
	if errors.As(err, &ce) && ce.Reason != "" {
		return ce.Reason
	}
	return "admin credential no longer valid"
}

// sendEvent delivers one event unless ctx is done first; a consumer that
// stopped reading cannot leak the pump goroutine once ctx cancels.
func sendEvent(ctx context.Context, ch chan<- AdminStreamEvent, ev AdminStreamEvent) bool {
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// sleepCtx waits d or until ctx is done; false = abandoned.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
