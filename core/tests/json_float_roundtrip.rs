use serde_json::{Value, json};

#[test]
fn finite_json_numbers_preserve_ieee_bits() -> Result<(), serde_json::Error> {
    for expected in [
        0.9199979180470109_f64,
        -0.9199979180470109,
        0.1,
        -0.0,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
    ] {
        let encoded = serde_json::to_vec(&json!({ "payload": { "center": { "y": expected } } }))?;
        let decoded: Value = serde_json::from_slice(&encoded)?;
        assert_eq!(
            decoded["payload"]["center"]["y"].as_f64().map(f64::to_bits),
            Some(expected.to_bits()),
            "JSON changed the IEEE bits of {expected:?}"
        );
    }
    Ok(())
}
