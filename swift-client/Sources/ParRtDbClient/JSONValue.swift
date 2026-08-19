import Foundation

/// The document currency — the `serde_json::Value` equivalent. User documents are
/// schemaless jsonb server-side, so they flow through this enum; consumer models
/// decode out of it. `int`/`double` stay distinct so int64-indexed fields survive
/// round-trips (see int64-indexable support in docs/superpowers/specs/).
///
/// Caveat: JSON has one number type, and Foundation emits the shortest number
/// form. An integral Double encodes without its fraction (`2.0` -> `2`) and
/// decoding tries Int64 first, so an integral `.double` comes back `.int` —
/// `.double(2.0)` does not survive a round-trip, and a server f64 with an
/// integral value arrives as `.int`. Double-typed consumers must read values
/// through `doubleValue`, which accepts both cases.
public enum JSONValue: Equatable, Hashable, Sendable, Codable {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    public var stringValue: String? {
        if case let .string(string) = self {
            return string
        }
        return nil
    }

    public var objectValue: [String: JSONValue]? {
        if case let .object(object) = self {
            return object
        }
        return nil
    }

    /// Tolerant Double accessor: `.int` and `.double` both yield a Double, so
    /// Double-typed consumers survive the integral-double collapse documented on
    /// the enum. All other cases return nil.
    public var doubleValue: Double? {
        switch self {
        case let .int(int): Double(int)
        case let .double(double): double
        default: nil
        }
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let bool = try? container.decode(Bool.self) {
            self = .bool(bool)
        } else if let int = try? container.decode(Int64.self) { // Int64 before Double
            self = .int(int)
        } else if let double = try? container.decode(Double.self) {
            self = .double(double)
        } else if let string = try? container.decode(String.self) {
            self = .string(string)
        } else if let array = try? container.decode([JSONValue].self) {
            self = .array(array)
        } else if let object = try? container.decode([String: JSONValue].self) {
            self = .object(object)
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "unsupported JSON value")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .null: try container.encodeNil()
        case let .bool(bool): try container.encode(bool)
        case let .int(int): try container.encode(int)
        case let .double(double): try container.encode(double)
        case let .string(string): try container.encode(string)
        case let .array(array): try container.encode(array)
        case let .object(object): try container.encode(object)
        }
    }

    /// Bridge from `JSONSerialization.jsonObject(with:)` output.
    public static func from(any: Any) throws -> JSONValue {
        switch any {
        case is NSNull: return .null
        case let number as NSNumber:
            // JSONSerialization booleans arrive as the CFBoolean singletons —
            // check before the number branches or `true` degrades to .int(1).
            if number === kCFBooleanTrue || number === kCFBooleanFalse {
                return .bool(number.boolValue)
            }
            // CFNumberType discrimination: keep Int64 and Double apart.
            if CFNumberIsFloatType(number) {
                return .double(number.doubleValue)
            }
            // Integral: reject outside Int64 rather than wrap (e.g. UInt64.max).
            let int64 = number.int64Value
            guard NSNumber(value: int64) == number else {
                throw CocoaError(
                    .propertyListReadCorrupt,
                    userInfo: [NSLocalizedDescriptionKey: "integer outside Int64 range"]
                )
            }
            return .int(int64)
        case let string as String: return .string(string)
        case let array as [Any]: return try .array(array.map(from(any:)))
        case let object as [String: Any]: return try .object(object.mapValues(from(any:)))
        default:
            throw CocoaError(
                .propertyListReadCorrupt,
                userInfo: [NSLocalizedDescriptionKey: "unserializable JSON value"]
            )
        }
    }

    /// Bridge back to a `JSONSerialization`-compatible Any.
    public var anyValue: Any {
        switch self {
        case .null: NSNull()
        case let .bool(bool): bool
        case let .int(int): int
        case let .double(double): double
        case let .string(string): string
        case let .array(array): array.map(\.anyValue)
        case let .object(object): object.mapValues(\.anyValue)
        }
    }
}

/// CodingKey that materializes any string, to enumerate raw payload keys.
/// Internal (not private): the tagged message enums in Wire.swift/Mutation.swift
/// reuse it for per-variant unknown-field validation.
struct AnyStringCodingKey: CodingKey {
    let stringValue: String
    var intValue: Int? {
        nil
    }

    init?(intValue _: Int) {
        nil
    }

    init(stringValue: String) {
        self.stringValue = stringValue
    }
}

public extension Decoder {
    /// serde `deny_unknown_fields` equivalent: throws if the payload carries a key
    /// not declared on `K`. Wire structs/enums call this FIRST in `init(from:)`,
    /// before building their typed container:
    /// `try decoder.rejectUnknownKeys("TypeName", as: CodingKeys.self)`.
    ///
    /// `K` must be a String-raw-value CodingKey enum that is also CaseIterable —
    /// declared exactly `enum CodingKeys: String, CodingKey, CaseIterable`.
    ///
    /// Deliberately NOT a KeyedDecodingContainer method: `allKeys` drops payload
    /// keys that don't materialize a `Key` case, so a strictly-keyed container can
    /// never see the unknown keys it must reject. Reading a second, permissively
    /// keyed container off the same Decoder enumerates the raw payload keys.
    func rejectUnknownKeys<K: CodingKey & RawRepresentable & CaseIterable>(
        _ typeName: String, as _: K.Type
    ) throws where K.RawValue == String {
        let allowed = Set(K.allCases.map(\.rawValue))
        let raw = try container(keyedBy: AnyStringCodingKey.self)
        for key in raw.allKeys where !allowed.contains(key.stringValue) {
            throw DecodingError.dataCorruptedError(
                forKey: key, in: raw,
                debugDescription: "\(typeName): unknown field '\(key.stringValue)'"
            )
        }
    }
}

// MARK: - WireEncodable

/// A Codable wire type's JSON-object view. The single source of truth is the
/// conformer's own `encode(to:)` — the default implementation round-trips
/// through JSONEncoder so callers always see exactly the bytes the encoder
/// produces, never a re-derivation. `Query` (Task 8) and `Transaction`
/// (Task 9) conform; any future wire struct gains `wireObject()` by declaring
/// conformance.
public protocol WireEncodable: Codable, Sendable {
    /// The wire encoding as a JSON object. Throws `RtDbError` (internal) when
    /// the conformer encodes to a non-object JSON value — unreachable for the
    /// keyed wire structs; the guard keeps the invariant explicit.
    func wireObject() throws -> [String: JSONValue]
}

public extension WireEncodable {
    func wireObject() throws -> [String: JSONValue] {
        let data = try JSONEncoder().encode(self)
        guard let object = try JSONDecoder().decode(JSONValue.self, from: data).objectValue else {
            throw RtDbError(code: .internal, message: "\(Self.self) encoded to a non-object JSON value")
        }
        return object
    }
}
