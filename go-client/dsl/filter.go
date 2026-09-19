// go-client/dsl/filter.go
package dsl

// Mirrors rust-client/src/query.rs's FilterExpr constructors — thin helpers
// so callers never hand-build tagged objects.

import "github.com/paulrobello/par-rt-db/go-client/wire"

// Eq returns the equality predicate.
func Eq(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterEq{Field: field, Value: value}
}

// Neq returns the inequality predicate.
func Neq(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterNeq{Field: field, Value: value}
}

// Gt returns the greater-than predicate.
func Gt(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterGt{Field: field, Value: value}
}

// Gte returns the greater-or-equal predicate.
func Gte(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterGte{Field: field, Value: value}
}

// Lt returns the less-than predicate.
func Lt(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterLt{Field: field, Value: value}
}

// Lte returns the less-or-equal predicate.
func Lte(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterLte{Field: field, Value: value}
}

// In returns the set-membership predicate (field equals any value).
func In(field string, values ...wire.JSONValue) wire.FilterExpr {
	return wire.FilterIn{Field: field, Values: values}
}

// Contains returns the array-membership predicate (value ∈ doc.field).
func Contains(field string, value wire.JSONValue) wire.FilterExpr {
	return wire.FilterContains{Field: field, Value: value}
}

// Exists returns the presence predicate (field present and non-null).
func Exists(field string) wire.FilterExpr {
	return wire.FilterExists{Field: field}
}

// OlderThan returns the execution-time-relative age predicate. Valid only
// in patchByQuery/deleteByQuery filters.
func OlderThan(field string, ms int64) wire.FilterExpr {
	return wire.FilterOlderThan{Field: field, Ms: ms}
}

// And returns the conjunction.
func And(exprs ...wire.FilterExpr) wire.FilterExpr {
	return wire.FilterAnd{Exprs: exprs}
}

// Or returns the disjunction.
func Or(exprs ...wire.FilterExpr) wire.FilterExpr {
	return wire.FilterOr{Exprs: exprs}
}

// Not returns the negation.
func Not(expr wire.FilterExpr) wire.FilterExpr {
	return wire.FilterNot{Expr: expr}
}
