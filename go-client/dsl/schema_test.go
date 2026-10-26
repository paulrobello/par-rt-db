// go-client/dsl/schema_test.go
package dsl

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestSchemaByNFixture(t *testing.T) {
	schema := DefineSchema()
	schema.Table("items", func(t *TableBuilder) {
		t.Field("name", Str()).
			Field("n", Int64())
		t.Index("by_n", "n")
	})
	b, err := json.Marshal(schema.Build())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"tables":{"items":{"fields":{"n":{"type":"int64"},"name":{"type":"string"}},"indexes":[{"fields":["n"],"name":"by_n"}]}}}`
	if string(b) != want {
		t.Fatalf("by_n fixture drifted:\n got %s\nwant %s", b, want)
	}
}

func TestSchemaRichTable(t *testing.T) {
	schema := DefineSchema()
	defaultDur := int64(86400000)
	schema.Table("tasks", func(t *TableBuilder) {
		t.Field("title", Str()).
			Field("owner", IdRefWithOnDelete("users", OnDeleteCascade)).
			Field("status", FieldLiteral(wire.String("open"))).
			Field("tags", ArrayOf(Str())).
			Field("embedding", VectorF(3)).
			Index("by_status", "status").
			Unique().
			SearchIndex("by_title", "english", "title").
			Trgm().
			VectorIndex("by_embedding", "embedding", 3, []string{"status"}, "").
			OwnerField("owner").
			TTL("expiresAt", &defaultDur).
			SoftDelete()
	})
	b, err := json.Marshal(schema.Build())
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range []string{
		`"onDelete":"cascade"`, `"type":"literal"`, `"type":"vector"`, `"dimensions":3`,
		`"unique":true`, `"search":true`, `"trgm":true`, `"language":"english"`, `"filterFields":["status"]`,
		`"ownerField":"owner"`, `"ttl":{"defaultDurationMs":86400000,"field":"expiresAt"}`,
		`"softDelete":true`,
	} {
		if !bytes.Contains(b, []byte(pin)) {
			t.Fatalf("missing pin %s in %s", pin, b)
		}
	}
}

func bytesHasHelper(b []byte, s string) bool { return bytes.Contains(b, []byte(s)) }

// TestSchemaDescribeEmitsOnlyWhenSet pins the wire contract of Describe:
// `description`/`fieldDescriptions` appear exactly when declared, and a
// description-less table carries neither key (core omits both when unset).
func TestSchemaDescribeEmitsOnlyWhenSet(t *testing.T) {
	schema := DefineSchema()
	schema.Table("items", func(t *TableBuilder) {
		t.Field("name", Str()).
			Field("qty", Num()).
			Describe("Inventory items", map[string]string{"name": "Display name"})
	})
	schema.Table("notes", func(t *TableBuilder) {
		t.Field("body", Str())
	})
	b, err := json.Marshal(schema.Build())
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range []string{
		`"description":"Inventory items"`,
		`"fieldDescriptions":{"name":"Display name"}`,
	} {
		if !bytesHasHelper(b, pin) {
			t.Fatalf("missing pin %s in %s", pin, b)
		}
	}
	for _, banned := range []string{
		`"notes":{"body":{"type":"string"},"description"`,
		`"notes":{"body":{"type":"string"},"fieldDescriptions"`,
	} {
		if bytesHasHelper(b, banned) {
			t.Fatalf("description key leaked into undescribed table: %s", b)
		}
	}
}

func TestTableBuilderInsertOnly(t *testing.T) {
	schema := DefineSchema()
	schema.Table("log", func(t *TableBuilder) {
		t.Field("n", Int64())
		t.InsertOnly()
	})
	schema.Table("plain", func(t *TableBuilder) { t.Field("n", Int64()) })
	b, err := json.Marshal(schema.Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"insertOnly":true`)) || bytes.Count(b, []byte("insertOnly")) != 1 {
		t.Fatalf("insertOnly emitted wrongly: %s", b)
	}
}
