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

	"github.com/paulrobello/par-rt-db/go-client/errors"
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

// ---------------------------------------------------------------------------
// ENH-052 family 2 — spec-table corpus: computed-field stamping
// (wire-corpus/spec/computed-stamping.json). The fixture pins this engine's
// stampComputed port of server `txn::stamp_computed` +
// `value_expr::eval_value_expr`: each row's wire schema fragment (`fields` +
// `computed`) is decoded through the engine's own parseTableDef, stamped with
// the engine, and asserted per key with jsonEq — a JSON-null expected value
// means the key must be ABSENT after stamping. A semantics change regenerates
// the fixture in the same commit (`make gen-spec-computed-stamping`).

func computedStampCorpusPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve corpus path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "wire-corpus", "spec", "computed-stamping.json")
}

func loadComputedStampRows(t *testing.T) []struct {
	ID       string          `json:"id"`
	Fields   json.RawMessage `json:"fields"`
	Computed json.RawMessage `json:"computed"`
	InputDoc json.RawMessage `json:"inputDoc"`
	Now      int64           `json:"now"`
	Expect   struct {
		StampedValues map[string]json.RawMessage `json:"stampedValues"`
		Error         bool                       `json:"error"`
	} `json:"expect"`
} {
	t.Helper()
	raw, err := os.ReadFile(computedStampCorpusPath(t))
	if err != nil {
		t.Fatalf("read computed stamping corpus: %v", err)
	}
	var doc struct {
		Rows []struct {
			ID       string          `json:"id"`
			Fields   json.RawMessage `json:"fields"`
			Computed json.RawMessage `json:"computed"`
			InputDoc json.RawMessage `json:"inputDoc"`
			Now      int64           `json:"now"`
			Expect   struct {
				StampedValues map[string]json.RawMessage `json:"stampedValues"`
				Error         bool                       `json:"error"`
			} `json:"expect"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse computed stamping corpus: %v", err)
	}
	return doc.Rows
}

// everyComputedStampOpHasAFixtureRow: the corpus README's own-op coverage
// rule — every ValueExpr op this engine's EvalValueExpr distinguishes must be
// exercised by a fixture row; a grammar node without a row fails.
func TestComputedStampingEveryOpHasAFixtureRow(t *testing.T) {
	rows := loadComputedStampRows(t)
	seen := map[string]bool{}
	for _, r := range rows {
		computedV, err := wire.UnmarshalJSON(r.Computed)
		if err != nil {
			t.Fatalf("decode computed: %v", err)
		}
		for _, cv := range computedV.(wire.Object) {
			raw, merr := json.Marshal(cv)
			if merr != nil {
				t.Fatalf("marshal computed entry: %v", merr)
			}
			expr, derr := wire.UnmarshalValueExpr(raw)
			if derr != nil {
				t.Fatalf("decode computed entry: %v", derr)
			}
			seen[valueExprOp(expr)] = true
		}
	}
	for _, op := range []string{
		"field", "literal", "concat", "add", "sub", "mul", "div",
		"coalesce", "lower", "upper", "trim", "cast", "now", "case",
	} {
		if !seen[op] {
			t.Fatalf("no fixture row exercises op '%s'", op)
		}
	}
}

// valueExprOp maps a decoded wire.ValueExpr back to its wire op tag, so the
// coverage check reads the fixture's own vocabulary.
func valueExprOp(expr wire.ValueExpr) string {
	switch expr.(type) {
	case wire.ValueField:
		return "field"
	case wire.ValueLiteral:
		return "literal"
	case wire.ValueConcat:
		return "concat"
	case wire.ValueAdd:
		return "add"
	case wire.ValueSub:
		return "sub"
	case wire.ValueMul:
		return "mul"
	case wire.ValueDiv:
		return "div"
	case wire.ValueCoalesce:
		return "coalesce"
	case wire.ValueLower:
		return "lower"
	case wire.ValueUpper:
		return "upper"
	case wire.ValueTrim:
		return "trim"
	case wire.ValueCast:
		return "cast"
	case wire.ValueNow:
		return "now"
	case wire.ValueCase:
		return "case"
	default:
		return "unknown"
	}
}

func TestComputedStampingRowsMatchThisEngine(t *testing.T) {
	for i, r := range loadComputedStampRows(t) {
		frag, err := wire.UnmarshalJSON(mustObject(t, r.Fields, r.Computed, i))
		if err != nil {
			t.Fatalf("row %d: encode fragment: %v", i, err)
		}
		tableDef, err := parseTableDef(frag)
		if err != nil {
			t.Fatalf("row %d: parse schema fragment: %v", i, err)
		}
		docV, err := wire.UnmarshalJSON(r.InputDoc)
		if err != nil {
			t.Fatalf("row %d: decode inputDoc: %v", i, err)
		}
		doc, _ := docV.(wire.Object)
		stamped, stampErr := stampComputed(tableDef, doc, r.Now)
		if r.Expect.Error {
			if stampErr == nil {
				t.Fatalf("row %d: expected the stamp to fail, got %+v", i, stamped)
			}
			if re, ok := stampErr.(*errors.RtDbError); ok && re.Code != errors.CodeBadRequest {
				t.Fatalf("row %d: error code %v, want BAD_REQUEST", i, re.Code)
			}
			continue
		}
		if stampErr != nil {
			t.Fatalf("row %d: unexpected stamp error: %v", i, stampErr)
		}
		for name, rawExpected := range r.Expect.StampedValues {
			expected, err := wire.UnmarshalJSON(rawExpected)
			if err != nil {
				t.Fatalf("row %d: decode expected %s: %v", i, name, err)
			}
			if _, isNull := expected.(wire.Null); isNull {
				if _, present := stamped[name]; present {
					t.Fatalf("row %d: key '%s' must be absent after stamping", i, name)
				}
				continue
			}
			got, present := stamped[name]
			if !present {
				t.Fatalf("row %d: missing computed key '%s'", i, name)
			}
			if !jsonEq(got, expected) {
				t.Fatalf("row %d: stamped value of '%s' is %s, want %s", i, name,
					mustMarshal(t, got), mustMarshal(t, expected))
			}
		}
	}
}

// mustObject wraps the fragment fields+computed pair into one table JSON
// object for parseTableDef.
func mustObject(t *testing.T, fields, computed json.RawMessage, i int) json.RawMessage {
	t.Helper()
	out, err := json.Marshal(map[string]json.RawMessage{"fields": fields, "computed": computed})
	if err != nil {
		t.Fatalf("row %d: marshal fragment: %v", i, err)
	}
	return out
}

func mustMarshal(t *testing.T, v wire.JSONValue) string {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(out)
}
