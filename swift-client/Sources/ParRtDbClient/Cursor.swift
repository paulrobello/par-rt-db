import Foundation

/// Pagination cursor codec — port of rust-client/src/cursor.rs, which mirrors
/// server/src/pagination.rs, ts-client/src/pagination.ts, and the python
/// client: a cursor is standard base64 (with padding) of the compact JSON
/// encoding of the sort-key array `[indexValues..., createdAt, id]`. Clients
/// normally pass cursors through opaquely; these helpers exist for parity, and
/// the encoding must stay byte-compatible across every client.
public func encodeCursor(_ values: [JSONValue]) -> String {
    let encoder = JSONEncoder()
    // serde_json parity: compact output, unescaped forward slashes, and
    // BTreeMap-ordered object keys. (Realistic cursor payloads are scalar
    // arrays, where these settings are no-ops.)
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    // [JSONValue] encodes unconditionally; the fallback only satisfies the type.
    let json = (try? encoder.encode(values)) ?? Data("[]".utf8)
    return json.base64EncodedString()
}

/// Decode an opaque cursor back into its sort-key values. nil on non-base64,
/// non-JSON, or non-array input.
public func decodeCursor(_ cursor: String) -> [JSONValue]? {
    guard let json = Data(base64Encoded: cursor) else { return nil }
    return try? JSONDecoder().decode([JSONValue].self, from: json)
}
