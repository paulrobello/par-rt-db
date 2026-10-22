//! Versioned par-mesh policy. NULL capability preserves legacy authority.

pub use par_rt_db_core::host_capability::HostCapability;
use serde_json::{Map, Value};

use crate::auth::PrincipalCtx;
use crate::dsl::{EqBind, Step};
use crate::error::RtDbError;
use crate::schema::{FieldType, TableDef};

pub fn component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

#[derive(Debug, PartialEq, Eq)]
struct Address<'a> {
    name: &'a str,
    host: &'a str,
    project: &'a str,
}

fn address(value: &str, wildcard: bool) -> Option<Address<'_>> {
    let (name, rest) = value.split_once('@')?;
    let (host, project) = rest.split_once(':')?;
    (component(host) && component(project) && (component(name) || (wildcard && name == "*")))
        .then_some(Address {
            name,
            host,
            project,
        })
}

fn text<'a>(doc: &'a Value, field: &str) -> Option<&'a str> {
    doc.get(field)?.as_str()
}

fn routing(doc: &Value) -> Option<Address<'_>> {
    let sender = address(text(doc, "from_addr")?, false)?;
    let target = address(text(doc, "target")?, false)?;
    let to = address(text(doc, "to_addr")?, true)?;
    (text(doc, "host")? == target.host
        && to.host == target.host
        && to.project == target.project
        && (to.name == "*" || to.name == target.name))
        .then_some(sender)
}

pub fn host_visible(doc: &Value, capability: Option<&HostCapability>, table: &str) -> bool {
    let Some(HostCapability::ParMeshV1 { host }) = capability else {
        return true;
    };
    if !component(host) {
        return false;
    }
    match table {
        "agents" => true,
        "messages" => routing(doc)
            .is_some_and(|sender| sender.host == host || text(doc, "host") == Some(host)),
        _ => false,
    }
}

fn denied() -> RtDbError {
    RtDbError::forbidden("operation is not permitted by host capability")
}

pub fn document_table(ctx: &PrincipalCtx, table: &str) -> Result<(), RtDbError> {
    if let Some(HostCapability::ParMeshV1 { host }) = &ctx.host_capability
        && (!component(host) || !matches!(table, "agents" | "messages"))
    {
        return Err(denied());
    }
    Ok(())
}

pub fn non_document(ctx: &PrincipalCtx) -> Result<(), RtDbError> {
    if ctx.host_capability.is_some() {
        Err(denied())
    } else {
        Ok(())
    }
}

pub fn operation(ctx: &PrincipalCtx, step: &Step) -> Result<(), RtDbError> {
    if ctx.host_capability.is_none() {
        return Ok(());
    }
    match step {
        Step::Insert { table, .. }
        | Step::Patch { table, .. }
        | Step::ExpectVersion { table, .. }
        | Step::ExpectFields { table, .. }
        | Step::ExpectAbsent { table, .. } => document_table(ctx, table),
        _ => Err(denied()),
    }
}

const AGENT_STRINGS: &[&str] = &[
    "addr",
    "name",
    "host",
    "project",
    "harness",
    "delivery",
    "status",
    "session_ref",
    "socket",
];
const MESSAGE_STRINGS: &[&str] = &[
    "msgid",
    "from_addr",
    "to_addr",
    "target",
    "host",
    "priority",
    "subject",
    "body",
    "trace",
    "state",
    "ack_note",
];

pub fn schema_contract(ctx: &PrincipalCtx, table: &str, def: &TableDef) -> Result<(), RtDbError> {
    if ctx.host_capability.is_none() {
        return Ok(());
    }
    document_table(ctx, table)?;
    let (strings, numbers): (&[&str], &[&str]) = if table == "agents" {
        (AGENT_STRINGS, &["last_seen_ms", "expires_ms"])
    } else {
        (MESSAGE_STRINGS, &["ts_ms", "delivered_ms", "acked_ms"])
    };
    if strings
        .iter()
        .any(|f| !matches!(def.fields.get(*f), Some(FieldType::String)))
        || numbers
            .iter()
            .any(|f| !matches!(def.fields.get(*f), Some(FieldType::Number)))
    {
        return Err(denied());
    }
    Ok(())
}

fn own_agent(doc: &Value, host: &str) -> bool {
    address(text(doc, "addr").unwrap_or(""), false).is_some_and(|a| {
        a.host == host
            && text(doc, "host") == Some(host)
            && text(doc, "name") == Some(a.name)
            && text(doc, "project") == Some(a.project)
    })
}

pub fn insert(ctx: &PrincipalCtx, table: &str, doc: &Map<String, Value>) -> Result<(), RtDbError> {
    let Some(HostCapability::ParMeshV1 { host }) = &ctx.host_capability else {
        return Ok(());
    };
    let doc = Value::Object(doc.clone());
    let allowed = match table {
        "agents" => own_agent(&doc, host),
        "messages" => {
            routing(&doc).is_some_and(|sender| sender.host == host)
                && text(&doc, "state") == Some("queued")
                && doc["delivered_ms"].as_f64() == Some(0.0)
                && doc["acked_ms"].as_f64() == Some(0.0)
                && text(&doc, "ack_note") == Some("")
                && matches!(text(&doc, "priority"), Some("next-turn" | "interrupt"))
        }
        _ => false,
    };
    if allowed { Ok(()) } else { Err(denied()) }
}

pub fn patch(
    ctx: &PrincipalCtx,
    table: &str,
    previous: &Value,
    candidate: &Map<String, Value>,
    fields: &Map<String, Value>,
) -> Result<(), RtDbError> {
    let Some(HostCapability::ParMeshV1 { host }) = &ctx.host_capability else {
        return Ok(());
    };
    let allowed = match table {
        "agents" => own_agent(previous, host) && own_agent(&Value::Object(candidate.clone()), host),
        "messages" => {
            routing(previous).is_some()
                && fields
                    .iter()
                    .all(|(key, value)| candidate.get(key) == Some(value))
                && previous.as_object().is_some_and(|old| {
                    candidate.len() == old.len()
                        && old.iter().all(|(key, value)| {
                            fields.contains_key(key) || candidate.get(key) == Some(value)
                        })
                })
                && text(previous, "host") == Some(host)
                && match fields.get("state").and_then(Value::as_str) {
                    Some("delivered") => {
                        text(previous, "state") == Some("queued")
                            && fields.len() == 2
                            && positive(fields.get("delivered_ms"))
                    }
                    Some("acked") => {
                        matches!(text(previous, "state"), Some("queued" | "delivered"))
                            && fields.len() == 3
                            && positive(fields.get("acked_ms"))
                            && fields.get("ack_note").is_some_and(Value::is_string)
                    }
                    _ => false,
                }
        }
        _ => false,
    };
    if allowed { Ok(()) } else { Err(denied()) }
}

fn positive(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_f64)
        .is_some_and(|n| n.is_finite() && n > 0.0)
}

/// Shared accumulator uses absolute `start + binds.len()` placeholders. Routing
/// grammar is ASCII and anchored identically to `address`; malformed rows fail closed.
pub fn sql_visibility(
    table: &str,
    ctx: &PrincipalCtx,
    start: usize,
    binds: &mut Vec<EqBind>,
) -> Result<Option<String>, RtDbError> {
    document_table(ctx, table)?;
    let Some(HostCapability::ParMeshV1 { host }) = &ctx.host_capability else {
        return Ok(None);
    };
    if table == "agents" {
        return Ok(None);
    }
    let ph = start + binds.len();
    binds.push(EqBind::Text(host.clone()));
    let component = "[a-z0-9][a-z0-9_-]{0,31}";
    let concrete = format!("^{component}@{component}:{component}$");
    let to = format!("^({component}|\\*)@{component}:{component}$");
    Ok(Some(format!(
        "(jsonb_typeof(doc->'from_addr') = 'string' AND jsonb_typeof(doc->'target') = 'string' AND jsonb_typeof(doc->'to_addr') = 'string' AND jsonb_typeof(doc->'host') = 'string' AND doc->>'from_addr' ~ '{concrete}' AND doc->>'target' ~ '{concrete}' AND doc->>'to_addr' ~ '{to}' AND doc->>'host' = split_part(split_part(doc->>'target', '@', 2), ':', 1) AND split_part(doc->>'to_addr', '@', 2) = split_part(doc->>'target', '@', 2) AND (split_part(doc->>'to_addr', '@', 1) = '*' OR split_part(doc->>'to_addr', '@', 1) = split_part(doc->>'target', '@', 1)) AND (doc->>'host' = ${ph} OR split_part(split_part(doc->>'from_addr', '@', 2), ':', 1) = ${ph}))"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn computed_receipt_cannot_override_requested_transition() {
        let mut ctx = PrincipalCtx::bypass();
        ctx.host_capability = Some(HostCapability::ParMeshV1 {
            host: "receiver".into(),
        });
        let previous = json!({"from_addr":"sender@sender:mesh","target":"worker@receiver:mesh","to_addr":"*@receiver:mesh","host":"receiver","state":"queued","delivered_ms":0});
        let fields = json!({"state":"delivered","delivered_ms":2});
        let mut candidate = previous.as_object().unwrap().clone();
        candidate.extend(fields.as_object().unwrap().clone());
        assert!(
            patch(
                &ctx,
                "messages",
                &previous,
                &candidate,
                fields.as_object().unwrap()
            )
            .is_ok()
        );
        candidate.insert("state".into(), json!("queued"));
        assert!(
            patch(
                &ctx,
                "messages",
                &previous,
                &candidate,
                fields.as_object().unwrap()
            )
            .is_err()
        );
    }
}
