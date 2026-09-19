// go-client/dsl/valueexpr.go
package dsl

// Mirrors rust-client's ValueExpr constructor helpers — thin wrappers so
// callers never hand-build tagged objects.

import "github.com/paulrobello/par-rt-db/go-client/wire"

// Field returns a declared-field reference.
func Field(name string) wire.ValueExpr {
	return wire.ValueField{Field: name}
}

// Literal returns a JSON literal.
func Literal(v wire.JSONValue) wire.ValueExpr {
	return wire.ValueLiteral{Value: v}
}

// Concat concatenates string operands.
func Concat(parts ...wire.ValueExpr) wire.ValueExpr {
	return wire.ValueConcat{Parts: parts}
}

// Add returns left + right.
func Add(left, right wire.ValueExpr) wire.ValueExpr {
	return wire.ValueAdd{Left: left, Right: right}
}

// Sub returns left - right.
func Sub(left, right wire.ValueExpr) wire.ValueExpr {
	return wire.ValueSub{Left: left, Right: right}
}

// Mul returns left * right.
func Mul(left, right wire.ValueExpr) wire.ValueExpr {
	return wire.ValueMul{Left: left, Right: right}
}

// Div returns left / right (by-zero errors server-side).
func Div(left, right wire.ValueExpr) wire.ValueExpr {
	return wire.ValueDiv{Left: left, Right: right}
}

// Coalesce returns the first non-null candidate.
func Coalesce(parts ...wire.ValueExpr) wire.ValueExpr {
	return wire.ValueCoalesce{Parts: parts}
}

// Lower lowercases the operand.
func Lower(v wire.ValueExpr) wire.ValueExpr {
	return wire.ValueLower{Value: v}
}

// Upper uppercases the operand.
func Upper(v wire.ValueExpr) wire.ValueExpr {
	return wire.ValueUpper{Value: v}
}

// Trim trims surrounding whitespace.
func Trim(v wire.ValueExpr) wire.ValueExpr {
	return wire.ValueTrim{Value: v}
}

// Cast coerces the operand to a scalar type.
func Cast(v wire.ValueExpr, to wire.Cast) wire.ValueExpr {
	return wire.ValueCast{Value: v, To: to}
}

// Now returns the current timestamp expression.
func Now() wire.ValueExpr {
	return wire.ValueNow{}
}

// Case returns the conditional; first matching when wins, otherwise is the
// fallback.
func Case(whens []wire.CaseWhen, otherwise wire.ValueExpr) wire.ValueExpr {
	return wire.ValueCase{Whens: whens, Otherwise: otherwise}
}

// When builds one CaseWhen branch.
func When(condition wire.FilterExpr, result wire.ValueExpr) wire.CaseWhen {
	return wire.CaseWhen{When: condition, Then: result}
}
