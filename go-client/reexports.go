// go-client/reexports.go
package parrtdb

// Root-package re-exports so common use needs one import. Types alias 1:1;
// the generic HTTP helpers re-export as generic wrapper functions (Go
// cannot use an uninstantiated generic function as a value, so the plan's
// `var Query = httpclient.Query` sketch became wrappers — ledger R13).

import (
	"context"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// --- wire: dynamic values and int64 ---
type (
	JSONValue = wire.JSONValue
	Null      = wire.Null
	Bool      = wire.Bool
	Number    = wire.Number
	String    = wire.String
	Array     = wire.Array
	Object    = wire.Object
	Int64     = wire.Int64
)

// ParseInt64 converts a wire Int64 to a Go int64.
func ParseInt64(v Int64) (int64, error) { return wire.ParseInt64(v) }

// PROTOCOL_VERSION mirrors the server's protocol version.
const PROTOCOL_VERSION = wire.PROTOCOL_VERSION

// --- wire: query vocabulary ---
// (wire.Query is deliberately NOT aliased here: the root name Query is the
// generic run-a-query function below, matching the spec's usage example.
// Construct queries via NewTableQuery/Build.)
type (
	Order             = wire.Order
	Paginate          = wire.Paginate
	SearchMode        = wire.SearchMode
	SearchQuery       = wire.SearchQuery
	VectorSearchQuery = wire.VectorSearchQuery
	HybridSearchQuery = wire.HybridSearchQuery
	AggregateOp       = wire.AggregateOp
	AggregateSpec     = wire.AggregateSpec
	AggregateGroup    = wire.AggregateGroup
	PaginatedResult   = wire.PaginatedResult
)

// Order directions.
const (
	OrderAsc  = wire.OrderAsc
	OrderDesc = wire.OrderDesc
)

// Aggregate operators.
const (
	AggSum   = wire.AggSum
	AggAvg   = wire.AggAvg
	AggMin   = wire.AggMin
	AggMax   = wire.AggMax
	AggCount = wire.AggCount
)

// --- wire: filter grammar ---
type (
	FilterExpr      = wire.FilterExpr
	FilterEq        = wire.FilterEq
	FilterNeq       = wire.FilterNeq
	FilterGt        = wire.FilterGt
	FilterGte       = wire.FilterGte
	FilterLt        = wire.FilterLt
	FilterLte       = wire.FilterLte
	FilterIn        = wire.FilterIn
	FilterContains  = wire.FilterContains
	FilterExists    = wire.FilterExists
	FilterOlderThan = wire.FilterOlderThan
	FilterAnd       = wire.FilterAnd
	FilterOr        = wire.FilterOr
	FilterNot       = wire.FilterNot
)

// --- wire: value-expr grammar ---
type (
	ValueExpr     = wire.ValueExpr
	ValueField    = wire.ValueField
	ValueLiteral  = wire.ValueLiteral
	ValueConcat   = wire.ValueConcat
	ValueAdd      = wire.ValueAdd
	ValueSub      = wire.ValueSub
	ValueMul      = wire.ValueMul
	ValueDiv      = wire.ValueDiv
	ValueCoalesce = wire.ValueCoalesce
	ValueLower    = wire.ValueLower
	ValueUpper    = wire.ValueUpper
	ValueTrim     = wire.ValueTrim
	ValueCast     = wire.ValueCast
	ValueNow      = wire.ValueNow
	ValueCase     = wire.ValueCase
	CaseWhen      = wire.CaseWhen
	Cast          = wire.Cast
)

// --- wire: mutation vocabulary ---
type (
	Transaction        = wire.Transaction
	Step               = wire.Step
	StepInsert         = wire.StepInsert
	StepPatch          = wire.StepPatch
	StepAdjustCounter  = wire.StepAdjustCounter
	StepReplace        = wire.StepReplace
	StepDelete         = wire.StepDelete
	StepUndelete       = wire.StepUndelete
	StepExpectVersion  = wire.StepExpectVersion
	StepExpectAbsent   = wire.StepExpectAbsent
	StepUpsert         = wire.StepUpsert
	StepPatchByQuery   = wire.StepPatchByQuery
	StepDeleteByQuery  = wire.StepDeleteByQuery
	StepSchedule       = wire.StepSchedule
	StepCancelSchedule = wire.StepCancelSchedule
	StepStartWorkflow  = wire.StepStartWorkflow
	StepCancelWorkflow = wire.StepCancelWorkflow
	ScheduleWhen       = wire.ScheduleWhen
	WhenAfterMs        = wire.WhenAfterMs
	WhenRunAt          = wire.WhenRunAt
	WhenCron           = wire.WhenCron
	WhenInterval       = wire.WhenInterval
	StepResult         = wire.StepResult
	WorkflowSpec       = wire.WorkflowSpec
)

// --- errors ---
type (
	ErrorCode = errors.ErrorCode
	RtDbError = errors.RtDbError
)

// Error codes.
const (
	CodeUnauthorized        = errors.CodeUnauthorized
	CodeForbidden           = errors.CodeForbidden
	CodeNotFound            = errors.CodeNotFound
	CodeSchemaViolation     = errors.CodeSchemaViolation
	CodePreconditionFailed  = errors.CodePreconditionFailed
	CodeBadRequest          = errors.CodeBadRequest
	CodeInternal            = errors.CodeInternal
	CodeRateLimited         = errors.CodeRateLimited
	CodeConflict            = errors.CodeConflict
	CodeQuotaExceeded       = errors.CodeQuotaExceeded
	CodeUnsupportedProtocol = errors.CodeUnsupportedProtocol
	CodeCursorExpired       = errors.CodeCursorExpired
)

// HTTPStatus maps a code to its canonical status.
func HTTPStatus(code ErrorCode) int { return errors.HTTPStatus(code) }

// IsCode reports whether err carries exactly this code.
func IsCode(err error, code ErrorCode) bool { return errors.IsCode(err, code) }

// --- dsl ---
type (
	TableQuery    = dsl.TableQuery
	Mutation      = dsl.Mutation
	FieldType     = dsl.FieldType
	SchemaBuilder = dsl.SchemaBuilder
)

// NewTableQuery starts a fluent query.
func NewTableQuery(table string) TableQuery { return dsl.NewTableQuery(table) }

// NewMutation starts a fluent transaction.
func NewMutation() Mutation { return dsl.NewMutation() }

// DefineSchema starts a schema builder.
func DefineSchema() *SchemaBuilder { return dsl.DefineSchema() }

// EncodeCursor base64-encodes a pagination keyset.
func EncodeCursor(values []JSONValue) (string, error) { return dsl.EncodeCursor(values) }

// DecodeCursor decodes an opaque cursor.
func DecodeCursor(s string) ([]JSONValue, error) { return dsl.DecodeCursor(s) }

// --- httpclient ---
type (
	Client = httpclient.Client
	Option = httpclient.Option
)

// NewClient builds an HTTP client for one database.
func NewClient(baseURL, db, token string, opts ...Option) *Client {
	return httpclient.NewClient(baseURL, db, token, opts...)
}

// Query runs one query, decoding into T.
func Query[T any](ctx context.Context, c *Client, q wire.Query) (T, error) {
	return httpclient.Query[T](ctx, c, q)
}

// QueryBatch runs several queries in one round trip. (httpclient.Result[T]
// is generic; it stays referenced through the httpclient package because
// the module's go directive predates generic type aliases.)
func QueryBatch[T any](ctx context.Context, c *Client, queries []wire.Query) ([]httpclient.Result[T], error) {
	return httpclient.QueryBatch[T](ctx, c, queries)
}
