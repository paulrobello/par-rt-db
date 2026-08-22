import Foundation

// MARK: - Cast

/// Closed set of sound coercions for `Directive.changeType` and
/// `ValueExpr.cast`. Mirrors rust-client/src/wire/admin.rs `Cast` (camelCase
/// wire tags: `toString`/`toNumber`/`toInt64`/`toBoolean`).
public enum Cast: String, Equatable, Codable, Sendable {
    /// Coerce to string.
    case toString
    /// Coerce to JSON number.
    case toNumber
    /// Coerce to 64-bit integer.
    case toInt64
    /// Coerce to boolean.
    case toBoolean
}

// MARK: - ValueExpr

/// A closed, typed expression grammar for `Directive.evalExpr`'s backfill
/// expression (ENH-020 Stage 1, closing SEC-107). Mirrors rust-client
/// `wire::admin::ValueExpr` byte-for-byte: internally tagged `"op"`,
/// camelCase tags, unknown fields rejected per variant. Every `literal`
/// compiles to a bound `$n` placeholder (as jsonb); every `field` resolves
/// through the table's `TableDef` and reads `doc->'field'`. There is
/// deliberately no subquery node, no function-call-by-name node, and no
/// raw-SQL escape — the grammar is closed, so the SEC-107 injection concern
/// cannot arise from a `ValueExpr` payload. The only way to reach raw SQL is
/// the deprecated `ExprSource.legacy` source, gated to the root admin key.
public indirect enum ValueExpr: Equatable, Codable, Sendable {
    /// A declared field on this table (validated against `TableDef`). Reads
    /// `doc->'field'` (jsonb).
    case field(field: String)
    /// Any JSON literal. Bound as `$n::jsonb`, so objects/arrays/null round-trip.
    case literal(value: JSONValue)
    /// String concatenation. Postgres `concat(...)`, which ignores NULL args
    /// (treats them as empty) — wrap operands in `coalesce` for explicit control.
    case concat(parts: [ValueExpr])
    /// Numeric addition. Operands are cast to `::numeric`; the result is a
    /// JSON number via the surrounding `to_jsonb`.
    case add(left: ValueExpr, right: ValueExpr)
    /// Subtraction (`left - right`).
    case sub(left: ValueExpr, right: ValueExpr)
    /// Multiplication (`left * right`).
    case mul(left: ValueExpr, right: ValueExpr)
    /// Division (`left / right`); by-zero errors at runtime — guard with
    /// `case`/`coalesce` when the divisor may be zero.
    case div(left: ValueExpr, right: ValueExpr)
    /// `COALESCE(parts...)` — first non-null, or null.
    case coalesce(parts: [ValueExpr])
    /// Text lowercase. Operand cast to `::text`.
    case lower(value: ValueExpr)
    /// Text uppercase.
    case upper(value: ValueExpr)
    /// Trim surrounding whitespace.
    case trim(value: ValueExpr)
    /// A closed scalar coercion. Reuses `Directive.changeType`'s `Cast`.
    case cast(value: ValueExpr, to: Cast)
    /// Current timestamp as epoch milliseconds (a JSON number).
    case now
    /// Conditional: first matching `when`'s `then`, else `otherwise`. Each
    /// `when` is a `FilterExpr` (field references schema-validated, values bound).
    case caseExpr(whens: [CaseWhen], otherwise: ValueExpr)

    enum CodingKeys: String, CodingKey {
        case op, field, value, parts, left, right, to, whens, otherwise
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("ValueExpr", tagKey: "op", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        func reject(_ allowed: Set<String>) throws {
            try rejectUnknownVariantFields(
                "ValueExpr", variant: payload.tag, keys: payload.keys, allowed: allowed
            )
        }
        switch payload.tag {
        case "field":
            try reject(["op", "field"])
            self = try .field(field: container.decode(String.self, forKey: .field))
        case "literal":
            try reject(["op", "value"])
            self = try .literal(value: container.decode(JSONValue.self, forKey: .value))
        case "concat":
            try reject(["op", "parts"])
            self = try .concat(parts: container.decode([ValueExpr].self, forKey: .parts))
        case "add":
            try reject(["op", "left", "right"])
            self = try .add(
                left: container.decode(ValueExpr.self, forKey: .left),
                right: container.decode(ValueExpr.self, forKey: .right)
            )
        case "sub":
            try reject(["op", "left", "right"])
            self = try .sub(
                left: container.decode(ValueExpr.self, forKey: .left),
                right: container.decode(ValueExpr.self, forKey: .right)
            )
        case "mul":
            try reject(["op", "left", "right"])
            self = try .mul(
                left: container.decode(ValueExpr.self, forKey: .left),
                right: container.decode(ValueExpr.self, forKey: .right)
            )
        case "div":
            try reject(["op", "left", "right"])
            self = try .div(
                left: container.decode(ValueExpr.self, forKey: .left),
                right: container.decode(ValueExpr.self, forKey: .right)
            )
        case "coalesce":
            try reject(["op", "parts"])
            self = try .coalesce(parts: container.decode([ValueExpr].self, forKey: .parts))
        case "lower":
            try reject(["op", "value"])
            self = try .lower(value: container.decode(ValueExpr.self, forKey: .value))
        case "upper":
            try reject(["op", "value"])
            self = try .upper(value: container.decode(ValueExpr.self, forKey: .value))
        case "trim":
            try reject(["op", "value"])
            self = try .trim(value: container.decode(ValueExpr.self, forKey: .value))
        case "cast":
            try reject(["op", "value", "to"])
            self = try .cast(
                value: container.decode(ValueExpr.self, forKey: .value),
                to: container.decode(Cast.self, forKey: .to)
            )
        case "now":
            try reject(["op"])
            self = .now
        case "case":
            try reject(["op", "whens", "otherwise"])
            self = try .caseExpr(
                whens: container.decode([CaseWhen].self, forKey: .whens),
                otherwise: container.decode(ValueExpr.self, forKey: .otherwise)
            )
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "ValueExpr: unknown op '\(unknown)'"
                )
            )
        }
    }

    // swiftlint:disable:next cyclomatic_complexity
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .field(field):
            try container.encode("field", forKey: .op)
            try container.encode(field, forKey: .field)
        case let .literal(value):
            try container.encode("literal", forKey: .op)
            try container.encode(value, forKey: .value)
        case let .concat(parts):
            try container.encode("concat", forKey: .op)
            try container.encode(parts, forKey: .parts)
        case let .add(left, right):
            try container.encode("add", forKey: .op)
            try container.encode(left, forKey: .left)
            try container.encode(right, forKey: .right)
        case let .sub(left, right):
            try container.encode("sub", forKey: .op)
            try container.encode(left, forKey: .left)
            try container.encode(right, forKey: .right)
        case let .mul(left, right):
            try container.encode("mul", forKey: .op)
            try container.encode(left, forKey: .left)
            try container.encode(right, forKey: .right)
        case let .div(left, right):
            try container.encode("div", forKey: .op)
            try container.encode(left, forKey: .left)
            try container.encode(right, forKey: .right)
        case let .coalesce(parts):
            try container.encode("coalesce", forKey: .op)
            try container.encode(parts, forKey: .parts)
        case let .lower(value):
            try container.encode("lower", forKey: .op)
            try container.encode(value, forKey: .value)
        case let .upper(value):
            try container.encode("upper", forKey: .op)
            try container.encode(value, forKey: .value)
        case let .trim(value):
            try container.encode("trim", forKey: .op)
            try container.encode(value, forKey: .value)
        case let .cast(value, to):
            try container.encode("cast", forKey: .op)
            try container.encode(value, forKey: .value)
            try container.encode(to, forKey: .to)
        case .now:
            try container.encode("now", forKey: .op)
        case let .caseExpr(whens, otherwise):
            try container.encode("case", forKey: .op)
            try container.encode(whens, forKey: .whens)
            try container.encode(otherwise, forKey: .otherwise)
        }
    }
}

/// One branch of `ValueExpr.caseExpr`. Wire shape `{when, then}` — mirrors
/// rust-client `wire::admin::CaseWhen` (camelCase, unknown fields rejected).
public struct CaseWhen: Equatable, Codable, Sendable {
    /// The branch condition.
    public var when: FilterExpr
    /// The value when it matches.
    public var then: ValueExpr

    public init(when: FilterExpr, then: ValueExpr) {
        self.when = when
        self.then = then
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case when, then
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("CaseWhen", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        when = try container.decode(FilterExpr.self, forKey: .when)
        then = try container.decode(ValueExpr.self, forKey: .then)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(when, forKey: .when)
        try container.encode(then, forKey: .then)
    }
}

// MARK: - ExprSource / CondSource (untagged dual-accept)

/// Dual-accept source for `Directive.evalExpr`'s `expr`: a typed `ValueExpr`
/// (the safe path) or a legacy raw-SQL string (the deprecated path, gated to
/// the root admin key — the SEC-107 boundary until the string form is
/// removed). Mirrors rust `wire::admin::ExprSource` (`#[serde(untagged)]`):
/// the typed arm is tried first; a string fails `ValueExpr` (an
/// internally-tagged object) and falls through to legacy, while a hostile
/// object that is not a valid `ValueExpr` fails BOTH arms and is rejected —
/// it does not silently become legacy.
public enum ExprSource: Equatable, Codable, Sendable {
    /// The safe typed expression.
    case typed(ValueExpr)
    /// Deprecated raw SQL (root admin key only).
    case legacy(String)

    public init(from decoder: Decoder) throws {
        let raw = try JSONValue(from: decoder)
        if case let .string(sql) = raw {
            self = .legacy(sql)
            return
        }
        let data = try JSONEncoder().encode(raw)
        do {
            self = try .typed(JSONDecoder().decode(ValueExpr.self, from: data))
        } catch {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "ExprSource: neither a ValueExpr nor a legacy string"
                )
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case let .typed(expr):
            try container.encode(expr)
        case let .legacy(sql):
            try container.encode(sql)
        }
    }
}

/// Dual-accept source for `Directive.evalExpr`'s `where`: a typed
/// `FilterExpr` or a legacy raw-SQL predicate string. Same untagged
/// discipline as `ExprSource` — typed first, strings are legacy, a hostile
/// object fails both arms. Mirrors rust `wire::admin::CondSource`.
public enum CondSource: Equatable, Codable, Sendable {
    /// The safe typed predicate.
    case typed(FilterExpr)
    /// Deprecated raw SQL (root admin key only).
    case legacy(String)

    public init(from decoder: Decoder) throws {
        let raw = try JSONValue(from: decoder)
        if case let .string(sql) = raw {
            self = .legacy(sql)
            return
        }
        let data = try JSONEncoder().encode(raw)
        do {
            self = try .typed(JSONDecoder().decode(FilterExpr.self, from: data))
        } catch {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "CondSource: neither a FilterExpr nor a legacy string"
                )
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case let .typed(expr):
            try container.encode(expr)
        case let .legacy(sql):
            try container.encode(sql)
        }
    }
}

// MARK: - Directive

/// One schema-migration step. Wire shape mirrors rust-client
/// `wire::admin::Directive` (itself the server's `migrate::Directive`):
/// internally tagged `"op"`, camelCase tags, unknown fields rejected per
/// variant (the same shape contract as the mutation `Step`).
/// `evalExpr.where` is the wire alias of rust's `where_clause` field;
/// `changeType.default` serializes as JSON `null` when nil (rust's plain
/// `Option` — only `evalExpr.where` is omitted when absent).
public enum Directive: Equatable, Codable, Sendable {
    /// Rename a field (re-keys indexes/defaults, keeps values).
    case renameField(table: String, from: String, to: String)
    /// Rename a table.
    case renameTable(from: String, to: String)
    /// Coerce a field to a new type via a closed cast; `default` substitutes
    /// for un-coercible rows (nil = roll back on any).
    case changeType(table: String, field: String, to: FieldType, cast: Cast, default: JSONValue?)
    /// Remove a field (destructive).
    case dropField(table: String, field: String)
    /// Remove a whole table (destructive).
    case dropTable(name: String)
    /// Remove an index.
    case dropIndex(table: String, name: String)
    /// Backfill a default onto rows missing the field.
    case setDefault(table: String, field: String, value: JSONValue)
    /// Compute and set a field from a typed (or legacy) expression.
    case evalExpr(table: String, set: String, expr: ExprSource, where: CondSource?)

    enum CodingKeys: String, CodingKey {
        case op, table, from, to, field, name, cast, set, expr, value
        case `default`
        case `where`
    }

    // swiftlint:disable:next function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("Directive", tagKey: "op", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        func reject(_ allowed: Set<String>) throws {
            try rejectUnknownVariantFields(
                "Directive", variant: payload.tag, keys: payload.keys, allowed: allowed
            )
        }
        switch payload.tag {
        case "renameField":
            try reject(["op", "table", "from", "to"])
            self = try .renameField(
                table: container.decode(String.self, forKey: .table),
                from: container.decode(String.self, forKey: .from),
                to: container.decode(String.self, forKey: .to)
            )
        case "renameTable":
            try reject(["op", "from", "to"])
            self = try .renameTable(
                from: container.decode(String.self, forKey: .from),
                to: container.decode(String.self, forKey: .to)
            )
        case "changeType":
            try reject(["op", "table", "field", "to", "cast", "default"])
            self = try .changeType(
                table: container.decode(String.self, forKey: .table),
                field: container.decode(String.self, forKey: .field),
                to: container.decode(FieldType.self, forKey: .to),
                cast: container.decode(Cast.self, forKey: .cast),
                default: container.decodeIfPresent(JSONValue.self, forKey: .default)
            )
        case "dropField":
            try reject(["op", "table", "field"])
            self = try .dropField(
                table: container.decode(String.self, forKey: .table),
                field: container.decode(String.self, forKey: .field)
            )
        case "dropTable":
            try reject(["op", "name"])
            self = try .dropTable(name: container.decode(String.self, forKey: .name))
        case "dropIndex":
            try reject(["op", "table", "name"])
            self = try .dropIndex(
                table: container.decode(String.self, forKey: .table),
                name: container.decode(String.self, forKey: .name)
            )
        case "setDefault":
            try reject(["op", "table", "field", "value"])
            self = try .setDefault(
                table: container.decode(String.self, forKey: .table),
                field: container.decode(String.self, forKey: .field),
                value: container.decode(JSONValue.self, forKey: .value)
            )
        case "evalExpr":
            try reject(["op", "table", "set", "expr", "where"])
            self = try .evalExpr(
                table: container.decode(String.self, forKey: .table),
                set: container.decode(String.self, forKey: .set),
                expr: container.decode(ExprSource.self, forKey: .expr),
                where: container.decodeIfPresent(CondSource.self, forKey: .where)
            )
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "Directive: unknown op '\(unknown)'"
                )
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .renameField(table, from, to):
            try container.encode("renameField", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(from, forKey: .from)
            try container.encode(to, forKey: .to)
        case let .renameTable(from, to):
            try container.encode("renameTable", forKey: .op)
            try container.encode(from, forKey: .from)
            try container.encode(to, forKey: .to)
        case let .changeType(table, field, to, cast, defaultValue):
            try container.encode("changeType", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(field, forKey: .field)
            try container.encode(to, forKey: .to)
            try container.encode(cast, forKey: .cast)
            try container.encode(defaultValue, forKey: .default) // plain Option: nil -> null
        case let .dropField(table, field):
            try container.encode("dropField", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(field, forKey: .field)
        case let .dropTable(name):
            try container.encode("dropTable", forKey: .op)
            try container.encode(name, forKey: .name)
        case let .dropIndex(table, name):
            try container.encode("dropIndex", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(name, forKey: .name)
        case let .setDefault(table, field, value):
            try container.encode("setDefault", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .evalExpr(table, set, expr, whereClause):
            try container.encode("evalExpr", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(set, forKey: .set)
            try container.encode(expr, forKey: .expr)
            try container.encodeIfPresent(whereClause, forKey: .where)
        }
    }
}

// MARK: - MigrateRequest

/// HTTP body for `POST /admin/db/{db}/migrate`. Mirrors rust
/// `wire::admin::MigrateRequestOwned` (camelCase; `dryRun` decodes as false
/// when absent and is always serialized).
public struct MigrateRequest: Equatable, Codable, Sendable {
    /// The migration steps, applied in order.
    public var directives: [Directive]
    /// Preview only: validate, derive, commit nothing.
    public var dryRun: Bool

    public init(directives: [Directive], dryRun: Bool = false) {
        self.directives = directives
        self.dryRun = dryRun
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case directives, dryRun
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        directives = try container.decode([Directive].self, forKey: .directives)
        dryRun = try container.decodeIfPresent(Bool.self, forKey: .dryRun) ?? false
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(directives, forKey: .directives)
        try container.encode(dryRun, forKey: .dryRun)
    }
}

// MARK: - Migration builder

/// Builder for a schema migration — an ordered list of `Directive`s the
/// server applies transactionally to transform a database's schema and
/// documents. The Swift mirror of rust-client/src/migration.rs `Migration`
/// (itself the port of ts-client's `Migration`): chain the per-directive
/// methods (value-semantics — every call returns a NEW builder), then
/// `build()` for the bare directives or `buildRequest()` for the full
/// request body.
public struct Migration: Sendable {
    /// The queued directives, in application order.
    private let pending: [Directive]
    /// Stashed for `buildRequest()`; `build()` discards it — the HTTP method
    /// takes `dryRun` as a separate argument.
    private let runsDry: Bool

    /// Start an empty migration (no directives, dry-run off).
    public init() {
        pending = []
        runsDry = false
    }

    private init(pending: [Directive], runsDry: Bool) {
        self.pending = pending
        self.runsDry = runsDry
    }

    private func adding(_ directive: Directive) -> Migration {
        Migration(pending: pending + [directive], runsDry: runsDry)
    }

    /// Queue a `renameField` directive — re-keys the field (and its index
    /// entries / defaults map) without touching stored values.
    public func renameField(_ table: String, from: String, to: String) -> Migration {
        adding(.renameField(table: table, from: from, to: to))
    }

    /// Queue a `renameTable` directive.
    public func renameTable(from: String, to: String) -> Migration {
        adding(.renameTable(from: from, to: to))
    }

    /// Queue a `changeType` directive — coerce `field` to `to` via the closed
    /// `cast`; `default` substitutes for un-coercible rows (without it one bad
    /// value rolls the whole migrate back atomically).
    public func changeType(
        _ table: String,
        field: String,
        to: FieldType,
        cast: Cast,
        default defaultValue: JSONValue? = nil
    ) -> Migration {
        adding(.changeType(table: table, field: field, to: to, cast: cast, default: defaultValue))
    }

    /// Queue a `dropField` directive (destructive).
    public func dropField(_ table: String, _ field: String) -> Migration {
        adding(.dropField(table: table, field: field))
    }

    /// Queue a `dropTable` directive (destructive).
    public func dropTable(_ name: String) -> Migration {
        adding(.dropTable(name: name))
    }

    /// Queue a `dropIndex` directive.
    public func dropIndex(_ table: String, _ name: String) -> Migration {
        adding(.dropIndex(table: table, name: name))
    }

    /// Queue a `setDefault` directive — stamps `value` onto every existing row
    /// missing the field.
    public func setDefault(_ table: String, field: String, value: JSONValue) -> Migration {
        adding(.setDefault(table: table, field: field, value: value))
    }

    /// Legacy raw-SQL `evalExpr` (deprecated, ENH-020 / SEC-107). Prefer
    /// `evalExprTyped(_:set:expr:where:)` — the typed `ValueExpr` path. This
    /// legacy form remains gated to the root admin key server-side.
    public func evalExpr(
        _ table: String, set: String, expr: String, where clause: String? = nil
    ) -> Migration {
        adding(
            .evalExpr(
                table: table,
                set: set,
                expr: .legacy(expr),
                where: clause.map { CondSource.legacy($0) }
            )
        )
    }

    /// Typed `evalExpr` (ENH-020, SEC-107 structural close). The safe path:
    /// `expr` is a closed `ValueExpr` grammar and `where` is an optional typed
    /// `FilterExpr`. The two sources may not mix — pass both typed, or use
    /// `evalExpr(_:set:expr:where:)` for the legacy raw-SQL form (never combine
    /// a typed `expr` with a legacy `where`, or vice versa).
    public func evalExprTyped(
        _ table: String, set: String, expr: ValueExpr, where clause: FilterExpr? = nil
    ) -> Migration {
        adding(
            .evalExpr(
                table: table,
                set: set,
                expr: .typed(expr),
                where: clause.map(CondSource.typed)
            )
        )
    }

    /// Stash the `dryRun` flag for `buildRequest()`. `build()` discards it —
    /// the HTTP method takes `dryRun` as a separate argument.
    public func dryRun(_ flag: Bool = true) -> Migration {
        Migration(pending: pending, runsDry: flag)
    }

    /// The ordered directives, ready for
    /// `RtDbAdminClient.migrateSchema(_:directives:dryRun:)`.
    public func build() -> [Directive] {
        pending
    }

    /// The full request body (directives + `dryRun`).
    public func buildRequest() -> MigrateRequest {
        MigrateRequest(directives: pending, dryRun: runsDry)
    }
}
