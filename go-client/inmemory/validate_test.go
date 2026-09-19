// Validator tests — the Go port of rust in_memory/tests/validate.rs.
package inmemory

import (
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestValidateDocRejectsUnknownField(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	bad := docObj("name", "a", "status", "todo", "order", int64(1), "bogus", int64(9))
	err := validateDoc(table, bad)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "bogus") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestValidateDocRejectsReservedField(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	bad := docObj("name", "a", "status", "todo", "order", int64(1), "_id", "x")
	err := validateDoc(table, bad)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "_id") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestValidateDocRejectsWrongFieldType(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	bad := docObj("name", int64(42), "status", "todo", "order", int64(1))
	err := validateDoc(table, bad)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "name") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestValidateDocRejectsMissingRequiredField(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	bad := docObj("name", "a", "order", int64(1)) // missing required "status"
	err := validateDoc(table, bad)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "status") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestValidateDocAcceptsValidDocWithOptionalAbsent(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	good := docObj("name", "a", "status", "todo", "order", int64(1))
	if err := validateDoc(table, good); err != nil {
		t.Fatalf("valid doc rejected: %v", err)
	}
}

func TestValidateDocAcceptsOptionalSetToNull(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	good := docObj("name", "a", "status", "todo", "order", int64(1), "note", nil)
	if err := validateDoc(table, good); err != nil {
		t.Fatalf("valid doc rejected: %v", err)
	}
}

func TestStripUnsetOptionalsDropsNullOptionalString(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	doc := docObj("name", "a", "status", "todo", "order", int64(1), "note", nil)
	stripped := stripUnsetOptionals(table, doc)
	if _, present := stripped["note"]; present {
		t.Fatalf("note key not stripped: %v", stripped)
	}
	if len(stripped) != 3 {
		t.Fatalf("unexpected keys: %v", stripped)
	}
}

func TestStripUnsetOptionalsKeepsNullForOptionalThatAcceptsNull(t *testing.T) {
	schema := parseSchemaOrDie(t, buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("t", func(tb *dsl.TableBuilder) { tb.Field("x", dsl.Optional(dsl.Null())) })
	}))
	doc := docObj("x", nil)
	stripped := stripUnsetOptionals(schema.Tables["t"], doc)
	if _, present := stripped["x"]; !present {
		t.Fatal("x key must be preserved (Optional<Null> accepts null)")
	}
}

func parseSchemaOrDie(t *testing.T, v wire.JSONValue) *SchemaDef {
	t.Helper()
	s, err := parseSchema(v)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return s
}

func TestIsHexIDChecks32LowercaseHexChars(t *testing.T) {
	if !isHexID(wire.String("0123456789abcdef0123456789abcdef")) {
		t.Fatal("valid hex id rejected")
	}
	if isHexID(wire.String("0123456789ABCDEF0123456789ABCDEF")) {
		t.Fatal("uppercase accepted")
	}
	if isHexID(wire.String("0123456789abcdef")) {
		t.Fatal("too-short accepted")
	}
	if isHexID(wire.Number("42")) {
		t.Fatal("number accepted")
	}
	if isHexID(wire.Null{}) {
		t.Fatal("null accepted")
	}
}

func TestIsInt64StringAcceptsI64RangeOnly(t *testing.T) {
	valid := []string{"0", "-1", "9223372036854775807", "-9223372036854775808"}
	invalid := []string{"9223372036854775808", "-9223372036854775809", "1.5", "-", ""}
	for _, s := range valid {
		if !isInt64String(wire.String(s)) {
			t.Fatalf("%q should be valid", s)
		}
	}
	for _, s := range invalid {
		if isInt64String(wire.String(s)) {
			t.Fatalf("%q should be invalid", s)
		}
	}
	if isInt64String(wire.Number("42")) {
		t.Fatal("number kind accepted")
	}
}

func TestIsBase64StringMatchesTheTSRegex(t *testing.T) {
	valid := []string{"", "ABCD", "ABC=", "AB==", "YWJjZA=="}
	invalid := []string{"ABC", "A===", "ABC!"}
	for _, s := range valid {
		if !isBase64String(wire.String(s)) {
			t.Fatalf("%q should be valid", s)
		}
	}
	for _, s := range invalid {
		if isBase64String(wire.String(s)) {
			t.Fatalf("%q should be invalid", s)
		}
	}
	if isBase64String(wire.Number("42")) {
		t.Fatal("number kind accepted")
	}
}

func TestValidateValueHandlesEachFieldTypeVariant(t *testing.T) {
	if !validateValue(FieldType{Kind: "string"}, wire.String("hi")) {
		t.Fatal("string")
	}
	if validateValue(FieldType{Kind: "string"}, wire.Number("2")) {
		t.Fatal("string accepts number")
	}
	if !validateValue(FieldType{Kind: "number"}, wire.Number("2.5")) {
		t.Fatal("number")
	}
	if !validateValue(FieldType{Kind: "boolean"}, wire.Bool(true)) {
		t.Fatal("boolean")
	}
	if !validateValue(FieldType{Kind: "null"}, wire.Null{}) {
		t.Fatal("null")
	}
	if !validateValue(FieldType{Kind: "any"}, wire.Null{}) {
		t.Fatal("any")
	}
	if !validateValue(FieldType{Kind: "id", Table: "x"}, wire.String("0123456789abcdef0123456789abcdef")) {
		t.Fatal("id")
	}
	if !validateValue(FieldType{Kind: "literal", Value: wire.String("a")}, wire.String("a")) {
		t.Fatal("literal")
	}
	if !validateValue(FieldType{Kind: "optional", Inner: &FieldType{Kind: "string"}}, wire.Null{}) {
		t.Fatal("optional null")
	}
	if !validateValue(FieldType{Kind: "union", Variants: []FieldType{{Kind: "string"}, {Kind: "number"}}}, wire.Number("2")) {
		t.Fatal("union")
	}
	if !validateValue(FieldType{Kind: "array", Inner: &FieldType{Kind: "number"}},
		wire.Array{wire.Number("1"), wire.Number("2"), wire.Number("3")}) {
		t.Fatal("array")
	}
	if !validateValue(FieldType{Kind: "int64"}, wire.String("42")) {
		t.Fatal("int64")
	}
	if !validateValue(FieldType{Kind: "bytes"}, wire.String("YWJjZA==")) {
		t.Fatal("bytes")
	}
	if !validateValue(FieldType{Kind: "vector", Dimensions: 3},
		wire.Array{wire.Number("1.0"), wire.Number("2.0"), wire.Number("3.0")}) {
		t.Fatal("vector")
	}
}

func TestCanonicalIsKeyOrderIndependent(t *testing.T) {
	a := docObj("b", int64(1), "a", int64(2))
	b := docObj("a", int64(2), "b", int64(1))
	if canonical(a) != canonical(b) {
		t.Fatal("canonical is key-order dependent")
	}
}

func TestApplyPatchMergesFieldsAndReValidatesWholeDoc(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	doc := docObj("name", "a", "status", "todo", "order", int64(1))
	merged, err := applyPatch(table, doc, docObj("order", int64(9)), 0)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if got := merged["order"]; !jsonEq(got, wire.Number("9")) {
		t.Fatalf("order not patched: %v", got)
	}
	if got := merged["name"]; !jsonEq(got, wire.String("a")) {
		t.Fatalf("non-patched field lost: %v", got)
	}
}

func TestApplyPatchNullOnOptionalInnerThatRejectsNullDeletesKey(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	doc := docObj("name", "a", "status", "todo", "order", int64(1), "note", "hi")
	merged, err := applyPatch(table, doc, docObj("note", nil), 0)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if _, present := merged["note"]; present {
		t.Fatalf("note key not stripped: %v", merged)
	}
}

func TestApplyPatchRejectsUnknownField(t *testing.T) {
	table := itemsTable(t, parsedTestSchema(t))
	doc := docObj("name", "a", "status", "todo", "order", int64(1))
	_, err := applyPatch(table, doc, docObj("bogus", int64(1)), 0)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "bogus") {
		t.Fatalf("got: %s", re.Message)
	}
}
