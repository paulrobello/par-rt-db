// Placeholder substitution — the corpus's three placeholders ($id labels are
// stripped at seed time by the harness; $idRef objects expand to the recorded
// minted id; $prev is the paginate-cursor sentinel handled by the harness's
// query executor).
package corpus

import (
	"fmt"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Substitute replaces every {"$idRef": "<label>"} object anywhere in the
// tree with the minted id recorded for that seed label. prev, when non-empty,
// additionally replaces the "$prev" string sentinel (the harness resolves it
// against the first page before calling this for the second pass — pass ""
// for the first-page run).
func Substitute(v wire.JSONValue, ids map[string]string, prev string) (wire.JSONValue, error) {
	switch t := v.(type) {
	case wire.Object:
		if len(t) == 1 {
			if labelV, ok := t["$idRef"]; ok {
				label, ok := labelV.(wire.String)
				if !ok {
					return nil, fmt.Errorf("$idRef label must be a string")
				}
				id, recorded := ids[string(label)]
				if !recorded {
					return nil, fmt.Errorf("$idRef references unknown seed label '%s'", string(label))
				}
				return wire.String(id), nil
			}
		}
		out := wire.Object{}
		for k, val := range t {
			if k == "cursor" {
				if s, isStr := val.(wire.String); isStr && string(s) == "$prev" {
					if prev == "" {
						return nil, fmt.Errorf("$prev sentinel resolved outside the two-pass query run")
					}
					out[k] = wire.String(prev)
					continue
				}
			}
			sv, err := Substitute(val, ids, prev)
			if err != nil {
				return nil, err
			}
			out[k] = sv
		}
		return out, nil
	case wire.Array:
		out := make(wire.Array, len(t))
		for i, item := range t {
			sv, err := Substitute(item, ids, prev)
			if err != nil {
				return nil, err
			}
			out[i] = sv
		}
		return out, nil
	default:
		return v, nil
	}
}
