package dev.parrtdb.inmemory

import dev.parrtdb.admin.Directive
import dev.parrtdb.admin.DirectiveReport
import dev.parrtdb.admin.MigrateResult
import dev.parrtdb.errors.badRequest
import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.CaseWhen
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Plans and applies a declarative migration. Structural directives fold into a working schema
 * copy while data directives rewrite the doc map. A failed directive is atomic: the doc store
 * is restored wholesale and the working schema is never installed. With [dryRun] the full plan
 * is validated and affected rows reported, but nothing commits.
 */
fun applyMigration(s: Store, directives: List<Directive>, dryRun: Boolean): MigrateResult {
    val fires: List<NotifyFire>
    val result: MigrateResult
    s.lock.lock()
    try {
        val current = s.schema ?: throw badRequest("no schema pushed for migration")
        var planned = current
        val touched = LinkedHashSet<String>()
        val reports = ArrayList<DirectiveReport>(directives.size)
        val snapshot = snapshotDocs(s)
        try {
            for (d in directives) {
                val (next, report, table) = applyDirective(s, planned, d)
                planned = next
                reports += report
                if (table.isNotEmpty()) touched += table
            }
            validateSchema(planned)
        } catch (e: Throwable) {
            s.docs = snapshot
            throw e
        }
        val schemaJson = schemaToWire(planned)
        if (dryRun) {
            s.docs = snapshot
            result = MigrateResult(false, schemaJson, reports)
        } else {
            s.schema = planned
            s.tables.clear()
            s.tables.putAll(planned.tables)
            notifySubs(s, touched)
            result = MigrateResult(true, schemaJson, reports)
        }
        fires = s.takePendingFires()
    } finally {
        s.lock.unlock()
    }
    for (f in fires) f.callback(f.value)
    return result
}

private data class Applied(val schema: SchemaDef, val report: DirectiveReport, val table: String)

private fun applyDirective(s: Store, planned: SchemaDef, d: Directive): Applied =
    when (d) {
        is Directive.RenameField -> applyRenameField(s, planned, d.table, d.from, d.to)
        is Directive.RenameTable -> applyRenameTable(s, planned, d.from, d.to)
        is Directive.ChangeType -> applyChangeType(s, planned, d)
        is Directive.DropField -> applyDropField(s, planned, d.table, d.field)
        is Directive.DropTable -> applyDropTable(s, planned, d.name)
        is Directive.DropIndex -> applyDropIndex(s, planned, d.table, d.name)
        is Directive.SetDefault -> applySetDefault(s, planned, d.table, d.field, d.value)
        is Directive.EvalExpr -> throw badRequest("evalExpr unsupported in-memory")
    }

private fun requireMigrateTable(schema: SchemaDef, name: String): TableDef =
    schema.tables[name] ?: throw badRequest("table '$name' does not exist")

private fun SchemaDef.withTable(name: String, td: TableDef): SchemaDef =
    SchemaDef(LinkedHashMap(tables).also { it[name] = td })

private fun tableRows(s: Store, table: String): List<StoredRow> = s.docs.entries.filter { it.key.table == table }.map { it.value }

private fun JsonObject.withField(name: String, value: JsonElement): JsonObject =
    JsonObject(LinkedHashMap(this).also { it[name] = value })

private fun JsonObject.withoutField(name: String): JsonObject = JsonObject(LinkedHashMap(this).also { it.remove(name) })

/** Rewrites every leaf field equal to [from] to [to] (server rename_filter_fields). */
internal fun renamedFilterExpr(expr: FilterExpr, from: String, to: String): FilterExpr {
    fun f(x: String) = if (x == from) to else x
    return when (expr) {
        is FilterExpr.And -> FilterExpr.And(expr.exprs.map { renamedFilterExpr(it, from, to) })
        is FilterExpr.Or -> FilterExpr.Or(expr.exprs.map { renamedFilterExpr(it, from, to) })
        is FilterExpr.Not -> FilterExpr.Not(renamedFilterExpr(expr.expr, from, to))
        is FilterExpr.In -> expr.copy(field = f(expr.field))
        is FilterExpr.Contains -> expr.copy(field = f(expr.field))
        is FilterExpr.Exists -> expr.copy(field = f(expr.field))
        is FilterExpr.OlderThan -> expr.copy(field = f(expr.field))
        is FilterExpr.Eq -> expr.copy(field = f(expr.field))
        is FilterExpr.Neq -> expr.copy(field = f(expr.field))
        is FilterExpr.Gt -> expr.copy(field = f(expr.field))
        is FilterExpr.Gte -> expr.copy(field = f(expr.field))
        is FilterExpr.Lt -> expr.copy(field = f(expr.field))
        is FilterExpr.Lte -> expr.copy(field = f(expr.field))
    }
}

/** Rewrites every field reference equal to [from] to [to] (server rename_value_expr_fields). */
internal fun renamedValueExpr(ve: ValueExpr, from: String, to: String): ValueExpr {
    fun r(x: ValueExpr) = renamedValueExpr(x, from, to)
    return when (ve) {
        is ValueExpr.Field -> if (ve.field == from) ValueExpr.Field(to) else ve
        is ValueExpr.Literal, ValueExpr.Now -> ve
        is ValueExpr.Concat -> ValueExpr.Concat(ve.parts.map(::r))
        is ValueExpr.Coalesce -> ValueExpr.Coalesce(ve.parts.map(::r))
        is ValueExpr.Add -> ValueExpr.Add(r(ve.left), r(ve.right))
        is ValueExpr.Sub -> ValueExpr.Sub(r(ve.left), r(ve.right))
        is ValueExpr.Mul -> ValueExpr.Mul(r(ve.left), r(ve.right))
        is ValueExpr.Div -> ValueExpr.Div(r(ve.left), r(ve.right))
        is ValueExpr.Lower -> ValueExpr.Lower(r(ve.value))
        is ValueExpr.Upper -> ValueExpr.Upper(r(ve.value))
        is ValueExpr.Trim -> ValueExpr.Trim(r(ve.value))
        is ValueExpr.CastTo -> ValueExpr.CastTo(r(ve.value), ve.to)
        is ValueExpr.Case ->
            ValueExpr.Case(ve.whens.map { CaseWhen(renamedFilterExpr(it.whenExpr, from, to), r(it.then)) }, r(ve.otherwise))
    }
}

/** Visits every field name a [ValueExpr] reads, including filter fields inside case whens. */
private fun walkValueExprFields(ve: ValueExpr, visit: (String) -> Unit) {
    when (ve) {
        is ValueExpr.Field -> visit(ve.field)
        is ValueExpr.Literal, ValueExpr.Now -> {}
        is ValueExpr.Concat -> ve.parts.forEach { walkValueExprFields(it, visit) }
        is ValueExpr.Coalesce -> ve.parts.forEach { walkValueExprFields(it, visit) }
        is ValueExpr.Add -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Sub -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Mul -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Div -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Lower -> walkValueExprFields(ve.value, visit)
        is ValueExpr.Upper -> walkValueExprFields(ve.value, visit)
        is ValueExpr.Trim -> walkValueExprFields(ve.value, visit)
        is ValueExpr.CastTo -> walkValueExprFields(ve.value, visit)
        is ValueExpr.Case -> {
            for (cw in ve.whens) {
                walkFilterExprFieldNames(cw.whenExpr, visit)
                walkValueExprFields(cw.then, visit)
            }
            walkValueExprFields(ve.otherwise, visit)
        }
    }
}

private fun applyRenameField(s: Store, planned: SchemaDef, table: String, from: String, to: String): Applied {
    val td = requireMigrateTable(planned, table)
    if (to in td.fields) throw badRequest("rename target '$table.$to' already exists")
    val fty = td.fields[from] ?: throw badRequest("renamed field '$table.$from' does not exist")
    fun rn(x: String?): String? = if (x == from) to else x
    val fields = LinkedHashMap(td.fields).also { it.remove(from); it[to] = fty }
    val indexes = td.indexes.map { idx -> idx.copy(fields = idx.fields.map { f -> if (f == from) to else f }) }
    val computed = LinkedHashMap<String, ValueExpr>()
    // ENH-028: the computed map follows the rename; every expression reference is rewritten.
    for ((k, v) in td.computed) computed[if (k == from) to else k] = renamedValueExpr(v, from, to)
    val defaults = LinkedHashMap<String, JsonElement>()
    for ((k, v) in td.defaults) defaults[if (k == from) to else k] = v
    val updated =
        td.copy(
            fields = fields,
            indexes = indexes,
            ownerField = rn(td.ownerField),
            collaboratorsField = rn(td.collaboratorsField),
            autoIncrementField = rn(td.autoIncrementField),
            updatedAtField = rn(td.updatedAtField),
            ttl = td.ttl?.let { if (it.field == from) it.copy(field = to) else it },
            authorize = td.authorize?.let { renamedFilterExpr(it, from, to) },
            computed = computed,
            defaults = defaults,
        )
    var affected = 0L
    for (row in tableRows(s, table)) {
        val value = row.doc[from] ?: continue
        row.doc = row.doc.withoutField(from).withField(to, value)
        affected++
    }
    return Applied(planned.withTable(table, updated), DirectiveReport("renameField", affected), table)
}

private fun applyRenameTable(s: Store, planned: SchemaDef, from: String, to: String): Applied {
    if (to in planned.tables) throw badRequest("rename target table '$to' already exists")
    val def = planned.tables[from] ?: throw badRequest("renamed table '$from' does not exist")
    val tables = LinkedHashMap<String, TableDef>()
    for ((name, td) in planned.tables) {
        if (name == from) continue
        // Id references to `from` follow the rename (onDelete preserved).
        val fields = LinkedHashMap<String, FieldType>()
        for ((fn, fty) in td.fields) fields[fn] = if (fty.kind == "id" && fty.table == from) fty.copy(table = to) else fty
        tables[name] = td.copy(fields = fields)
    }
    tables[to] = def
    val keys = s.docs.keys.filter { it.table == from }
    for (key in keys) {
        val row = s.docs.remove(key) ?: continue
        s.docs[RowKey(to, key.id)] = row
    }
    return Applied(SchemaDef(tables), DirectiveReport("renameTable", 0), to)
}

private fun castValidFor(cast: Cast, old: FieldType): Boolean {
    val tag = old.kind
    return when (cast) {
        Cast.ToString -> tag == "string" || tag == "number" || tag == "boolean" || tag == "int64"
        Cast.ToNumber -> tag == "string" || tag == "boolean" || tag == "int64"
        Cast.ToInt64 -> tag == "string" || tag == "number"
        Cast.ToBoolean -> tag == "string" || tag == "number"
    }
}

/** Pure coercion (server migrate::coerce_value): null when [value] cannot coerce under [cast]. */
internal fun coerceValue(cast: Cast, value: JsonElement): JsonElement? {
    val prim = value as? JsonPrimitive
    return when (cast) {
        Cast.ToString -> valueText(value)?.let { jsonStr(it) }
        Cast.ToNumber ->
            when {
                prim == null -> null
                prim.isString -> {
                    val trimmed = prim.content.trim()
                    val f = if (trimmed.isEmpty()) null else strictParseFloat(trimmed)
                    if (f == null || f.isNaN() || f.isInfinite()) null else jsonNumberFromF64(f)
                }
                isJsonNumber(prim) -> value
                prim.content == "true" -> jsonNum("1")
                prim.content == "false" -> jsonNum("0")
                else -> null
            }
        Cast.ToInt64 ->
            when {
                prim == null -> null
                prim.isString -> if (isInt64String(prim)) value else null
                isJsonNumber(prim) -> parseI64(prim.content)?.let { jsonStr(formatI64(it)) }
                else -> null
            }
        Cast.ToBoolean ->
            when {
                prim == null -> null
                prim.isString ->
                    when (prim.content.lowercase()) {
                        "true", "1" -> jsonBool(true)
                        "false", "0" -> jsonBool(false)
                        else -> null
                    }
                isJsonNumber(prim) -> jsonBool(jsonNumberF64(prim) != 0.0)
                else -> null
            }
    }
}

private fun applyChangeType(s: Store, planned: SchemaDef, t: Directive.ChangeType): Applied {
    val td = requireMigrateTable(planned, t.table)
    val oldTy = td.fields[t.field] ?: throw badRequest("changed field '${t.table}.${t.field}' does not exist")
    if (!castValidFor(t.cast, oldTy)) throw badRequest("cast ${t.cast.wire} is not valid for ${t.table}.${t.field}")
    val newTy = parseFieldType(t.to)
    var affected = 0L
    for (row in tableRows(s, t.table).sortedBy { it.id }) {
        val original = row.doc[t.field] ?: continue
        affected++
        val coerced = coerceValue(t.cast, original)
        if (coerced != null) {
            row.doc = row.doc.withField(t.field, coerced)
            continue
        }
        if (t.default != null) {
            row.doc = row.doc.withField(t.field, coerceValue(t.cast, t.default) ?: t.default)
            continue
        }
        throw badRequest("changeType cannot coerce value in ${t.table}.${row.id} (${canonical(original)}) and no default given")
    }
    val fields = LinkedHashMap(td.fields).also { it[t.field] = newTy }
    return Applied(planned.withTable(t.table, td.copy(fields = fields)), DirectiveReport("changeType", affected), t.table)
}

private fun applyDropField(s: Store, planned: SchemaDef, table: String, field: String): Applied {
    val td = requireMigrateTable(planned, table)
    if (field !in td.fields) throw badRequest("dropped field '$table.$field' does not exist")
    // ENH-028: a computed expression reading the dropped field would dangle.
    for ((name, expr) in td.computed) {
        var referenced = false
        walkValueExprFields(expr) { if (it == field) referenced = true }
        if (referenced) {
            throw badRequest("cannot drop field '$table.$field': it is referenced by computed field '$table.$name'; drop the computed field first")
        }
    }
    val updated =
        td.copy(
            fields = LinkedHashMap(td.fields).also { it.remove(field) },
            indexes = td.indexes.map { idx -> idx.copy(fields = idx.fields.filter { it != field }) },
            ownerField = td.ownerField.takeIf { it != field },
            collaboratorsField = td.collaboratorsField.takeIf { it != field },
            computed = LinkedHashMap(td.computed).also { it.remove(field) },
        )
    var affected = 0L
    for (row in tableRows(s, table)) {
        if (row.doc.containsKey(field)) {
            row.doc = row.doc.withoutField(field)
            affected++
        }
    }
    return Applied(planned.withTable(table, updated), DirectiveReport("dropField", affected), table)
}

private fun applyDropTable(s: Store, planned: SchemaDef, name: String): Applied {
    requireMigrateTable(planned, name)
    val keys = s.docs.keys.filter { it.table == name }
    keys.forEach { s.docs.remove(it) }
    val tables = LinkedHashMap(planned.tables).also { it.remove(name) }
    return Applied(SchemaDef(tables), DirectiveReport("dropTable", keys.size.toLong()), name)
}

private fun applyDropIndex(s: Store, planned: SchemaDef, table: String, name: String): Applied {
    val td = requireMigrateTable(planned, table)
    if (td.indexes.none { it.name == name }) throw badRequest("dropped index '$table.$name' does not exist")
    return Applied(planned.withTable(table, td.copy(indexes = td.indexes.filter { it.name != name })), DirectiveReport("dropIndex", 0), table)
}

private fun applySetDefault(s: Store, planned: SchemaDef, table: String, field: String, value: JsonElement): Applied {
    val td = requireMigrateTable(planned, table)
    if (field !in td.fields) throw badRequest("setDefault target '$table.$field' does not exist")
    var affected = 0L
    for (row in tableRows(s, table)) {
        if (!row.doc.containsKey(field)) {
            row.doc = row.doc.withField(field, value)
            affected++
        }
    }
    return Applied(planned, DirectiveReport("setDefault", affected), table)
}
