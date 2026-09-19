// go-client/wsclient/transport.go
package wsclient

// The dial/conn seam (mirrors rust ws.rs's transport boundary): a tiny
// interface so tests inject a scripted peer, with the coder/websocket
// implementation as the only third-party import in the module.

import (
	"context"

	"github.com/coder/websocket"
)

// transport is the connection seam.
type transport interface {
	Read(ctx context.Context) (jsonMessageType, []byte, error)
	Write(ctx context.Context, data []byte) error
	Close() error
}

// jsonMessageType distinguishes text frames; binary is unused by the
// protocol but part of the seam.
type jsonMessageType int

const (
	msgText jsonMessageType = iota
	msgClose
)

// wsTransport adapts *websocket.Conn to the seam.
type wsTransport struct {
	conn *websocket.Conn
}

func (t *wsTransport) Read(ctx context.Context) (jsonMessageType, []byte, error) {
	_, data, err := t.conn.Read(ctx)
	if err != nil {
		return msgClose, nil, err
	}
	return msgText, data, nil
}

func (t *wsTransport) Write(ctx context.Context, data []byte) error {
	return t.conn.Write(ctx, websocket.MessageText, data)
}

func (t *wsTransport) Close() error {
	// CloseNow (no handshake): the manager's Read must unblock immediately,
	// and a graceful echo depends on the peer reading, which cannot be
	// assumed during teardown.
	return t.conn.CloseNow()
}

// dial opens a /sync websocket.
func dial(ctx context.Context, wsURL string) (transport, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, err
	}
	return &wsTransport{conn: conn}, nil
}
