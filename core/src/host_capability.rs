//! Host capability vocabulary shared by the server and the Rust client.
//!
//! A [`HostCapability`] is an explicit, versioned, host-scoped application
//! capability carried on a machine token. It is a narrow discriminated
//! record — not a client-supplied policy language: unknown capability types,
//! unknown properties, and malformed hosts all fail closed.

use serde::{Deserialize, Serialize};

/// A versioned capability attached to a machine token, serialized with a
/// camelCase `"type"` tag — e.g. `{"type":"parMeshV1","host":"macdaddy"}`.
///
/// Deserialization is strict: an unknown tag, an unknown property, a missing
/// property, or a malformed `host` is rejected rather than silently dropped.
/// serde derive cannot enforce `deny_unknown_fields` on an internally tagged
/// enum, so `Deserialize` is implemented by hand over `serde_json::Value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum HostCapability {
    /// The par-mesh host capability: the token represents exactly one
    /// authenticated host.
    ParMeshV1 {
        /// The authenticated host name, in par-mesh's component grammar
        /// (`^[a-z0-9][a-z0-9-_]{0,31}$`).
        host: String,
    },
}

impl<'de> Deserialize<'de> for HostCapability {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("capability must be a JSON object"))?;
        let tag = obj
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("capability is missing its \"type\" tag"))?;
        match tag {
            "parMeshV1" => {
                if obj.len() != 2 {
                    return Err(serde::de::Error::custom(
                        "parMeshV1 capability accepts exactly the properties {\"type\", \"host\"}",
                    ));
                }
                let host = obj
                    .get("host")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        serde::de::Error::custom("parMeshV1 capability requires a string \"host\"")
                    })?;
                validate_host(host).map_err(serde::de::Error::custom)?;
                Ok(HostCapability::ParMeshV1 {
                    host: host.to_string(),
                })
            }
            other => Err(serde::de::Error::custom(format!(
                "unknown capability type '{other}'"
            ))),
        }
    }
}

/// Validates `host` against par-mesh's component grammar
/// `^[a-z0-9][a-z0-9-_]{0,31}$`: a lowercase ASCII letter or digit first,
/// then lowercase letters/digits/hyphens/underscores, at most 32 characters
/// in total. Returns the rejection reason on failure.
pub fn validate_host(host: &str) -> Result<(), String> {
    let mut chars = host.chars();
    let starts_ok = matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit());
    let rest_ok =
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
    if host.is_empty() || host.len() > 32 || !starts_ok || !rest_ok {
        return Err(format!(
            "invalid host '{host}': expected ^[a-z0-9][a-z0-9-_]{{0,31}}$"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn par_mesh_v1_serializes_with_the_camel_type_tag() {
        let cap = HostCapability::ParMeshV1 {
            host: "macdaddy".to_string(),
        };
        assert_eq!(
            serde_json::to_value(&cap).unwrap(),
            json!({"type": "parMeshV1", "host": "macdaddy"})
        );
    }

    #[test]
    fn capability_round_trips_through_serde() {
        let cap = HostCapability::ParMeshV1 {
            host: "macdaddy".to_string(),
        };
        let text = serde_json::to_string(&cap).unwrap();
        let back: HostCapability = serde_json::from_str(&text).unwrap();
        assert_eq!(back, cap);
    }

    #[test]
    fn unknown_capability_type_is_denied() {
        let err = serde_json::from_value::<HostCapability>(json!({
            "type": "parMeshV2",
            "host": "macdaddy"
        }))
        .unwrap_err();
        assert!(err.to_string().contains("unknown capability type"));
    }

    #[test]
    fn unknown_property_is_denied() {
        let err = serde_json::from_value::<HostCapability>(json!({
            "type": "parMeshV1",
            "host": "macdaddy",
            "admin": true
        }))
        .unwrap_err();
        assert!(err.to_string().contains("exactly"));
    }

    #[test]
    fn missing_host_is_denied() {
        assert!(
            serde_json::from_value::<HostCapability>(json!({
                "type": "parMeshV1"
            }))
            .is_err()
        );
    }

    #[test]
    fn non_string_host_is_denied() {
        assert!(
            serde_json::from_value::<HostCapability>(json!({
                "type": "parMeshV1",
                "host": 7
            }))
            .is_err()
        );
    }

    #[test]
    fn malformed_host_is_denied() {
        for bad in [
            "Macdaddy",      // uppercase
            "-leading",      // must start with a letter/digit
            "",              // empty
            "a b",           // space
            &"a".repeat(33), // over the 32-char cap
            "hôte",          // non-ASCII
        ] {
            assert!(validate_host(bad).is_err(), "host '{bad}' must be rejected");
            assert!(
                serde_json::from_value::<HostCapability>(json!({
                    "type": "parMeshV1",
                    "host": bad
                }))
                .is_err(),
                "capability with host '{bad}' must be rejected"
            );
        }
    }

    #[test]
    fn host_grammar_boundaries_are_accepted() {
        for good in ["a", "0", "macdaddy", "a-b_c9", &"a".repeat(32)] {
            assert_eq!(validate_host(good), Ok(()), "host '{good}' must pass");
        }
    }
}
