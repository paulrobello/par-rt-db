package dev.parrtdb.inmemory

import dev.parrtdb.errors.badRequest
import kotlinx.serialization.json.JsonElement

/** Values a finite literal-union (or lone literal) accepts; null when the type is not a finite set. */
private fun literalSet(ty: FieldType): List<JsonElement?>? =
    when (ty.kind) {
        "literal" -> listOf(ty.value)
        "union" ->
            if (ty.variants.isEmpty() || ty.variants.any { it.kind != "literal" }) null else ty.variants.map { it.value }
        else -> null
    }

/**
 * True when [next] is a safe widening of [old] (server schema::is_widening_of): finite literal-set
 * growth, or adding optional fields to a nested object shape.
 */
private fun isWideningOf(old: FieldType, next: FieldType): Boolean {
    val oldValues = literalSet(old)
    val newValues = literalSet(next)
    if (oldValues != null && newValues != null && oldValues.all { ov -> newValues.any { nv -> jsonEq(nv, ov) } }) return true
    return isObjectShapeWidening(old, next)
}

private fun isObjectShapeWidening(old: FieldType, next: FieldType): Boolean {
    if (old.kind == "array" && next.kind == "array") return isObjectShapeWidening(old.inner!!, next.inner!!)
    if (old.kind == "object" && next.kind == "object") {
        for ((name, oldTy) in old.fields) {
            val newTy = next.fields[name] ?: return false
            if (!stripOnDelete(newTy).equalTo(stripOnDelete(oldTy)) && !isWideningOf(oldTy, newTy)) return false
        }
        for ((name, newTy) in next.fields) if (name !in old.fields && newTy.kind != "optional") return false
        return true
    }
    return false
}

/** Strips onDelete from id fields, recursing through every compositor: adding or changing an action is additive. */
private fun stripOnDelete(ty: FieldType): FieldType =
    when (ty.kind) {
        "id" -> if (ty.onDelete == null) ty else FieldType("id", table = ty.table)
        "optional", "array", "record" -> FieldType(ty.kind, inner = stripOnDelete(ty.inner!!))
        "union" -> FieldType("union", variants = ty.variants.map { stripOnDelete(it) })
        "object" -> FieldType("object", fields = ty.fields.mapValues { stripOnDelete(it.value) })
        else -> ty
    }

/**
 * FM-30: flips trgm to true on any search index in [next] that was already a search index in [old]
 * and did not declare trgm on this push. Returns the adjusted schema (Go mutates in place). A no-op
 * when [old] is null. Must run before [detectDestructiveChanges], which deliberately ignores trgm.
 */
fun grandfatherTrgm(old: SchemaDef?, next: SchemaDef): SchemaDef {
    if (old == null) return next
    val tables =
        next.tables.mapValues { (tableName, newTable) ->
            val oldTable = old.tables[tableName] ?: return@mapValues newTable
            newTable.copy(
                indexes =
                    newTable.indexes.map { ni ->
                        if (!ni.search || ni.trgm) {
                            ni
                        } else if (oldTable.indexes.any { it.name == ni.name && it.search }) {
                            ni.copy(trgm = true)
                        } else {
                            ni
                        }
                    },
            )
        }
    return SchemaDef(tables)
}

private fun equalVectorSpec(a: VectorIndexSpec?, b: VectorIndexSpec?): Boolean =
    if (a == null || b == null) a == null && b == null else a.dimensions == b.dimensions && a.metric == b.metric && a.filterFields == b.filterFields

/**
 * Rejects a second push that removes or retypes any existing table, field or index
 * (server ddl.rs::detect_destructive_changes). Throws BAD_REQUEST with the server's messages.
 */
fun detectDestructiveChanges(old: SchemaDef, next: SchemaDef) {
    for ((tableName, oldTable) in old.tables) {
        val newTable = next.tables[tableName] ?: throw badRequest("removed table '$tableName'")
        for ((fieldName, oldType) in oldTable.fields) {
            val newType = newTable.fields[fieldName] ?: throw badRequest("removed field '$tableName.$fieldName'")
            val changed = !stripOnDelete(newType).equalTo(stripOnDelete(oldType))
            if (changed && !isWideningOf(oldType, newType)) throw badRequest("changed type of field '$tableName.$fieldName'")
        }
        for (oi in oldTable.indexes) {
            val ni = newTable.indexes.firstOrNull { it.name == oi.name } ?: throw badRequest("removed index '${oi.name}'")
            if (ni.fields != oi.fields) throw badRequest("changed fields of index '${oi.name}'")
            if (ni.search != oi.search) throw badRequest("changed kind of index '${oi.name}' (btree <-> search)")
            if (!equalVectorSpec(ni.vector, oi.vector)) throw badRequest("changed vector spec of index '${oi.name}'")
            if (ni.unique != oi.unique) throw badRequest("changed uniqueness of index '${oi.name}'")
            if (!jsonEq(ni.whereClause?.toJson(), oi.whereClause?.toJson())) throw badRequest("changed partial predicate of index '${oi.name}'")
            if (ni.language != oi.language) throw badRequest("changed language of search index '${oi.name}'")
        }
    }
}

/**
 * Installs [schema] as the sole database schema, merging additively on later pushes: documents and
 * idempotency entries are preserved and the denormalized table map is repopulated. Validation runs
 * first, so an invalid TTL or non-indexable index field fails SCHEMA_VIOLATION like the live server.
 */
fun Store.pushSchema(schema: JsonElement) {
    val parsed = parseSchema(schema)
    locked {
        validateSchema(parsed)
        validateOnDelete(parsed)
        val adjusted = grandfatherTrgm(this.schema, parsed)
        this.schema?.let { detectDestructiveChanges(it, adjusted) }
        this.schema = adjusted
        for ((name, def) in adjusted.tables) tables[name] = def
    }
}
