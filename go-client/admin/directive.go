// go-client/admin/directive.go
package admin

// Mirrors rust-client/src/wire/admin.rs's Directive — the schema-migration
// step union (tag "op", camelCase, strict per-variant decode). ExprSource /
// CondSource are the ENH-020 dual-accepts: a typed expression (safe) or a
// legacy raw-SQL string (deprecated, root admin key only).

import (
	"encoding/json"
	"fmt"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Directive is one schema-migration step. Variants: DirectiveRenameField /
// DirectiveRenameTable / DirectiveChangeType / DirectiveDropField /
// DirectiveDropTable / DirectiveDropIndex / DirectiveSetDefault /
// DirectiveEvalExpr.
type Directive interface{ isDirective() }

// Mirrors migrate.rs::Directive::renameField.
type DirectiveRenameField struct {
	Table string `json:"table"`
	From  string `json:"from"`
	To    string `json:"to"`
}

func (DirectiveRenameField) isDirective() {}

func (v DirectiveRenameField) MarshalJSON() ([]byte, error) {
	type alias DirectiveRenameField
	return wire.MarshalTagged("op", "renameField", alias(v))
}

// Mirrors migrate.rs::Directive::renameTable.
type DirectiveRenameTable struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (DirectiveRenameTable) isDirective() {}

func (v DirectiveRenameTable) MarshalJSON() ([]byte, error) {
	type alias DirectiveRenameTable
	return wire.MarshalTagged("op", "renameTable", alias(v))
}

// Mirrors migrate.rs::Directive::changeType. To is a dsl field-type value
// (dsl.Str(), dsl.Num(), …); Cast is the closed coercion set; Default
// substitutes for un-coercible rows (nil = roll back on any).
type DirectiveChangeType struct {
	Table   string         `json:"table"`
	Field   string         `json:"field"`
	To      wire.JSONValue `json:"to"`
	Cast    wire.Cast      `json:"cast"`
	Default wire.JSONValue `json:"default,omitempty"`
}

func (DirectiveChangeType) isDirective() {}

func (v DirectiveChangeType) MarshalJSON() ([]byte, error) {
	type alias DirectiveChangeType
	return wire.MarshalTagged("op", "changeType", alias(v))
}

// Mirrors migrate.rs::Directive::dropField.
type DirectiveDropField struct {
	Table string `json:"table"`
	Field string `json:"field"`
}

func (DirectiveDropField) isDirective() {}

func (v DirectiveDropField) MarshalJSON() ([]byte, error) {
	type alias DirectiveDropField
	return wire.MarshalTagged("op", "dropField", alias(v))
}

// Mirrors migrate.rs::Directive::dropTable.
type DirectiveDropTable struct {
	Name string `json:"name"`
}

func (DirectiveDropTable) isDirective() {}

func (v DirectiveDropTable) MarshalJSON() ([]byte, error) {
	type alias DirectiveDropTable
	return wire.MarshalTagged("op", "dropTable", alias(v))
}

// Mirrors migrate.rs::Directive::dropIndex.
type DirectiveDropIndex struct {
	Table string `json:"table"`
	Name  string `json:"name"`
}

func (DirectiveDropIndex) isDirective() {}

func (v DirectiveDropIndex) MarshalJSON() ([]byte, error) {
	type alias DirectiveDropIndex
	return wire.MarshalTagged("op", "dropIndex", alias(v))
}

// Mirrors migrate.rs::Directive::setDefault.
type DirectiveSetDefault struct {
	Table string         `json:"table"`
	Field string         `json:"field"`
	Value wire.JSONValue `json:"value"`
}

func (DirectiveSetDefault) isDirective() {}

func (v DirectiveSetDefault) MarshalJSON() ([]byte, error) {
	type alias DirectiveSetDefault
	return wire.MarshalTagged("op", "setDefault", alias(v))
}

// Mirrors migrate.rs::Directive::evalExpr. Expr is the write-source
// expression (dual-accept); Where is the optional dual-accept predicate.
type DirectiveEvalExpr struct {
	Table string      `json:"table"`
	Set   string      `json:"set"`
	Expr  ExprSource  `json:"expr"`
	Where *CondSource `json:"where,omitempty"`
}

func (DirectiveEvalExpr) isDirective() {}

func (v DirectiveEvalExpr) MarshalJSON() ([]byte, error) {
	type alias DirectiveEvalExpr
	return wire.MarshalTagged("op", "evalExpr", alias(v))
}

// UnmarshalDirective routes a tagged directive to its variant.
func UnmarshalDirective(data []byte) (Directive, error) {
	tag, err := wire.PeekTag(data, "op")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "renameField":
		return wire.DecodeTagged[DirectiveRenameField](data, "op")
	case "renameTable":
		return wire.DecodeTagged[DirectiveRenameTable](data, "op")
	case "changeType":
		return wire.DecodeTagged[DirectiveChangeType](data, "op")
	case "dropField":
		return wire.DecodeTagged[DirectiveDropField](data, "op")
	case "dropTable":
		return wire.DecodeTagged[DirectiveDropTable](data, "op")
	case "dropIndex":
		return wire.DecodeTagged[DirectiveDropIndex](data, "op")
	case "setDefault":
		return wire.DecodeTagged[DirectiveSetDefault](data, "op")
	case "evalExpr":
		return wire.DecodeTagged[DirectiveEvalExpr](data, "op")
	default:
		return nil, fmt.Errorf("admin: unknown directive op %q", tag)
	}
}

// ExprSource is evalExpr's expr: a typed wire.ValueExpr (the safe path) or
// a legacy raw-SQL string (deprecated, root admin key only).
type ExprSource struct {
	Typed  wire.ValueExpr
	Legacy string
}

// MarshalJSON emits the typed expr, or the quoted legacy string.
func (e ExprSource) MarshalJSON() ([]byte, error) {
	if e.Typed != nil {
		return json.Marshal(e.Typed)
	}
	return json.Marshal(e.Legacy)
}

// UnmarshalJSON dual-accepts: object → ValueExpr, string → legacy.
func (e *ExprSource) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		e.Typed, e.Legacy = nil, s
		return nil
	}
	expr, err := wire.UnmarshalValueExpr(b)
	if err != nil {
		return err
	}
	e.Typed, e.Legacy = expr, ""
	return nil
}

// CondSource is evalExpr's where: a typed wire.FilterExpr or a legacy
// raw-SQL predicate string.
type CondSource struct {
	Typed  wire.FilterExpr
	Legacy string
}

// MarshalJSON emits the typed predicate, or the quoted legacy string.
func (c CondSource) MarshalJSON() ([]byte, error) {
	if c.Typed != nil {
		return json.Marshal(c.Typed)
	}
	return json.Marshal(c.Legacy)
}

// UnmarshalJSON dual-accepts: object → FilterExpr, string → legacy.
func (c *CondSource) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		c.Typed, c.Legacy = nil, s
		return nil
	}
	expr, err := wire.UnmarshalFilterExpr(b)
	if err != nil {
		return err
	}
	c.Typed, c.Legacy = expr, ""
	return nil
}

// UnmarshalJSON decodes the dynamic to/default blobs (strict).
func (v *DirectiveChangeType) UnmarshalJSON(b []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		Table   string          `json:"table"`
		Field   string          `json:"field"`
		To      json.RawMessage `json:"to"`
		Cast    wire.Cast       `json:"cast"`
		Default json.RawMessage `json:"default,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	to, err := wire.UnmarshalJSON(r.To)
	if err != nil {
		return err
	}
	v.Table, v.Field, v.To, v.Cast = r.Table, r.Field, to, r.Cast
	if len(r.Default) > 0 {
		def, err := wire.UnmarshalJSON(r.Default)
		if err != nil {
			return err
		}
		v.Default = def
	}
	return nil
}

// UnmarshalJSON decodes the dynamic value blob (strict).
func (v *DirectiveSetDefault) UnmarshalJSON(b []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		Table string          `json:"table"`
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := wire.UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Table, v.Field, v.Value = r.Table, r.Field, val
	return nil
}
