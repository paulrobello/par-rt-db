// ENH-052 family 1 — spec-table corpus runner: the FieldType -> Postgres
// column-type mapping (wire-corpus/spec/index-column-types.json).
//
// Asserts the engine's indexColumnType against every fixture row — decoded
// through the engine's own parseFieldType, the same wire spelling pushSchema
// parses — and that the engine's FieldType variant set is covered by the
// fixture. columnType carries the server's raw Postgres string; the engine's
// four-value PgType maps back to it.
package inmemory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func specCorpusPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve corpus path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "wire-corpus", "spec", "index-column-types.json")
}

// specRow is one fixture row, its FieldType payload decoded twice: through the
// engine's own parseFieldType (for the mapping assertion) and kept as the raw
// wire tree (for the coverage lookup).
type specRow struct {
	fieldType     *FieldType
	fieldTypeWire wire.JSONValue
	columnType    string
	nullable      bool
	hasError      bool
}

func loadSpecRows(t *testing.T) []specRow {
	t.Helper()
	raw, err := os.ReadFile(specCorpusPath(t))
	if err != nil {
		t.Fatalf("read spec corpus: %v", err)
	}
	var doc struct {
		Rows []struct {
			FieldType  json.RawMessage `json:"fieldType"`
			ColumnType string          `json:"columnType"`
			Nullable   bool            `json:"nullable"`
			Error      bool            `json:"error"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse spec corpus: %v", err)
	}
	rows := make([]specRow, 0, len(doc.Rows))
	for _, r := range doc.Rows {
		ftv, err := wire.UnmarshalJSON(r.FieldType)
		if err != nil {
			t.Fatalf("decode row fieldType: %v", err)
		}
		ft, err := parseFieldType(ftv)
		if err != nil {
			t.Fatalf("parse row fieldType: %v", err)
		}
		rows = append(rows, specRow{
			fieldType:     ft,
			fieldTypeWire: ftv,
			columnType:    r.ColumnType,
			nullable:      r.Nullable,
			hasError:      r.Error,
		})
	}
	return rows
}

// rawPg maps the engine's four-value storage enum back to the server's raw
// Postgres column strings.
func rawPg(pg PgType) string {
	switch pg {
	case PgText:
		return "text"
	case PgNumber:
		return "double precision"
	case PgBoolean:
		return "boolean"
	case PgInt64:
		return "bigint"
	}
	return ""
}

func ftObj(kvs ...any) wire.JSONValue {
	m := map[string]wire.JSONValue{}
	for i := 0; i+1 < len(kvs); i += 2 {
		m[kvs[i].(string)] = kvs[i+1].(wire.JSONValue)
	}
	return wire.Object(m)
}

// ownVariants: one representative per FieldType variant this engine's
// indexColumnType distinguishes, in the wire spelling (the corpus README's
// own-variant coverage rule: a variant without a fixture row fails).
func ownVariants() []wire.JSONValue {
	return []wire.JSONValue{
		ftObj("type", wire.String("string")),
		ftObj("type", wire.String("number")),
		ftObj("type", wire.String("boolean")),
		ftObj("type", wire.String("int64")),
		ftObj("type", wire.String("id"), "table", wire.String("t")),
		ftObj("type", wire.String("id"), "table", wire.String("t"), "onDelete", wire.String("cascade")),
		ftObj("type", wire.String("literal"), "value", wire.String("x")),
		ftObj("type", wire.String("literal"), "value", wire.Number("42")),
		ftObj("type", wire.String("optional"), "inner", ftObj("type", wire.String("string"))),
		ftObj("type", wire.String("union"), "variants", wire.Array([]wire.JSONValue{
			ftObj("type", wire.String("literal"), "value", wire.String("a")),
			ftObj("type", wire.String("literal"), "value", wire.String("b")),
		})),
		ftObj("type", wire.String("union"), "variants", wire.Array([]wire.JSONValue{})),
		ftObj("type", wire.String("union"), "variants", wire.Array([]wire.JSONValue{
			ftObj("type", wire.String("literal"), "value", wire.String("a")),
			ftObj("type", wire.String("number")),
		})),
		ftObj("type", wire.String("null")),
		ftObj("type", wire.String("array"), "element", ftObj("type", wire.String("string"))),
		ftObj("type", wire.String("object"), "fields", ftObj("a", ftObj("type", wire.String("string")))),
		ftObj("type", wire.String("bytes")),
		ftObj("type", wire.String("any")),
		ftObj("type", wire.String("record"), "value", ftObj("type", wire.String("string"))),
		ftObj("type", wire.String("vector"), "dimensions", wire.Number("3")),
	}
}

func TestSpecCorpusOwnVariantsCovered(t *testing.T) {
	rows := loadSpecRows(t)
	variants := ownVariants()
	if len(variants) < 15 {
		t.Fatalf("own-variant list shrank: %d representatives", len(variants))
	}
	for _, own := range variants {
		found := false
		for _, r := range rows {
			if strictValueEq(own, r.fieldTypeWire) {
				found = true
				break
			}
		}
		if !found {
			asJSON, _ := json.Marshal(own)
			t.Fatalf("no fixture row for own variant %s", asJSON)
		}
	}
}

func TestSpecCorpusRowsMatchThisEngine(t *testing.T) {
	for i, r := range loadSpecRows(t) {
		resolved, err := indexColumnType(*r.fieldType)
		if r.hasError {
			if err == nil {
				t.Fatalf("row %d: expected an error for a non-indexable type, got %+v", i, resolved)
			}
			continue
		}
		if err != nil {
			t.Fatalf("row %d: unexpected error: %v", i, err)
		}
		if got := rawPg(resolved.Pg); got != r.columnType {
			t.Fatalf("row %d: columnType %q, want %q", i, got, r.columnType)
		}
		if resolved.Nullable != r.nullable {
			t.Fatalf("row %d: nullable %v, want %v", i, resolved.Nullable, r.nullable)
		}
	}
}
