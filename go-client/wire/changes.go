// go-client/wire/changes.go
package wire

// Mirrors the change-feed read shapes: server/src/change_log.rs::ChangeRow
// and server/src/protocol.rs::ChangeFeedResponse (GET /api/db/{db}/changes).

import "encoding/json"

// ChangeKind is a ChangeOp's lowercase op kind (the OpKind wire form).
type ChangeKind string

const (
	ChangeKindInsert  ChangeKind = "insert"
	ChangeKindPatch   ChangeKind = "patch"
	ChangeKindReplace ChangeKind = "replace"
	ChangeKindDelete  ChangeKind = "delete"
	ChangeKindUpsert  ChangeKind = "upsert"
)

// ChangeOp is one committed document op. Doc is the id's end-of-txn
// post-image, or Null when the row carries no visible end state (a delete, a
// txn-local insert+delete, or a payload-less migrate backfill) — the field is
// always present on the wire (no skip_serializing_if).
type ChangeOp struct {
	Seq   int64      `json:"seq"`
	Table string     `json:"table"`
	DocID string     `json:"docId"`
	Kind  ChangeKind `json:"kind"`
	Doc   JSONValue  `json:"doc"`
	Ts    int64      `json:"ts"`
}

// UnmarshalJSON decodes the dynamic post-image blob (strict).
func (o *ChangeOp) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Seq   int64           `json:"seq"`
		Table string          `json:"table"`
		DocID string          `json:"docId"`
		Kind  ChangeKind      `json:"kind"`
		Doc   json.RawMessage `json:"doc"`
		Ts    int64           `json:"ts"`
	}](b)
	if err != nil {
		return err
	}
	var doc JSONValue = Null{}
	if len(r.Doc) > 0 {
		if doc, err = UnmarshalJSON(r.Doc); err != nil {
			return err
		}
	}
	o.Seq, o.Table, o.DocID, o.Kind, o.Doc, o.Ts = r.Seq, r.Table, r.DocID, r.Kind, doc, r.Ts
	return nil
}

// MarshalJSON always emits doc (null when absent).
func (o ChangeOp) MarshalJSON() ([]byte, error) {
	doc := o.Doc
	if doc == nil {
		doc = Null{}
	}
	return json.Marshal(struct {
		Seq   int64      `json:"seq"`
		Table string     `json:"table"`
		DocID string     `json:"docId"`
		Kind  ChangeKind `json:"kind"`
		Doc   JSONValue  `json:"doc"`
		Ts    int64      `json:"ts"`
	}{o.Seq, o.Table, o.DocID, o.Kind, doc, o.Ts})
}

// ChangeFeedResponse is one page of ops strictly after the requested cursor,
// oldest first. NextSeq is the last returned seq only on a full page,
// otherwise Head; LogID changes when the db was dropped and recreated under
// the same name (the consumer must resync).
type ChangeFeedResponse struct {
	Ops     []ChangeOp `json:"ops"`
	NextSeq int64      `json:"nextSeq"`
	Head    int64      `json:"head"`
	LogID   string     `json:"logId"`
}

// UnmarshalJSON rejects unknown fields.
func (c *ChangeFeedResponse) UnmarshalJSON(b []byte) error {
	type alias ChangeFeedResponse
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	if v.Ops == nil {
		v.Ops = []ChangeOp{}
	}
	*c = ChangeFeedResponse(v)
	return nil
}
