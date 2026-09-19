// go-client/optimistic/layer.go
package optimistic

// The Layer glues the pure projection to a mutation applier: overlay first,
// roll back on applier error, and let the authoritative queryUpdate replace
// the overlay afterwards.

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// timeNowMs is the overlay timestamp source (wall clock; overlays are
// reconciled by the authoritative update anyway).
func timeNowMs() int64 { return time.Now().UnixMilli() }

// MutationApplier is the minimal interface an HTTP or WS client satisfies.
type MutationApplier interface {
	Mutate(ctx context.Context, txn wire.Transaction, idempotencyKey string) ([]wire.StepResult, error)
}

// Layer overlays projected results for tracked queries.
type Layer struct {
	applier MutationApplier

	mu    sync.Mutex
	cache map[string]wire.JSONValue // canonical query marshal → cached result
}

// New builds a Layer over an applier.
func New(applier MutationApplier) *Layer {
	return &Layer{applier: applier, cache: map[string]wire.JSONValue{}}
}

// Observe records the authoritative result for a query (call on every
// queryUpdate so overlays reconcile).
func (l *Layer) Observe(query wire.Query, value wire.JSONValue) {
	key, err := canonicalKey(query)
	if err != nil {
		return
	}
	l.mu.Lock()
	l.cache[key] = value
	l.mu.Unlock()
}

// Apply projects txn onto every tracked query that the txn touches, sends
// the mutation, and restores the pre-projection values on failure.
func (l *Layer) Apply(ctx context.Context, txn wire.Transaction, queries []wire.Query, idempotencyKey string) ([]wire.StepResult, error) {
	now := timeNowMs()
	type overlay struct {
		key  string
		Prev wire.JSONValue
		Next wire.JSONValue
	}
	var overlays []overlay
	l.mu.Lock()
	for _, q := range queries {
		key, err := canonicalKey(q)
		if err != nil {
			continue
		}
		last, ok := l.cache[key]
		if !ok {
			continue
		}
		proj := Project(q, last, txn, now)
		if proj.Kind == ProjectionOverlaid {
			overlays = append(overlays, overlay{key: key, Prev: last, Next: proj.Value})
			l.cache[key] = proj.Value
		}
	}
	l.mu.Unlock()
	results, err := l.applier.Mutate(ctx, txn, idempotencyKey)
	if err != nil {
		l.mu.Lock()
		for _, o := range overlays {
			l.cache[o.key] = o.Prev
		}
		l.mu.Unlock()
		return nil, err
	}
	return results, nil
}

// canonicalKey marshals the query — the marshal output is canonical.
func canonicalKey(q wire.Query) (string, error) {
	b, err := json.Marshal(q)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
