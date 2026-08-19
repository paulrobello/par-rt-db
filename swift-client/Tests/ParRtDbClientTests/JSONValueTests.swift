import Foundation
@testable import ParRtDbClient
import Testing

struct JSONValueTests {
    @Test func int64AndDoubleStayDistinct() throws {
        let json = Data(#"[1, 1.5, 9223372036854775807]"#.utf8)
        let roundTripped = try JSONDecoder().decode([JSONValue].self, from: json)
        #expect(roundTripped[0] == .int(1))
        #expect(roundTripped[1] == .double(1.5))
        #expect(roundTripped[2] == .int(9_223_372_036_854_775_807))
        let reencoded = try JSONEncoder().encode(roundTripped)
        let back = try JSONDecoder().decode([JSONValue].self, from: reencoded)
        #expect(back == roundTripped)
    }

    @Test func objectRoundTripsThroughSerialization() throws {
        let original: [String: JSONValue] = [
            "a": .int(1), "b": .string("x"), "c": .null, "d": .array([.bool(true), .double(2.5)])
        ]
        let value = JSONValue.object(original)
        let data = try JSONEncoder().encode(value)
        let parsed = try JSONDecoder().decode(JSONValue.self, from: data)
        #expect(parsed == value)
        #expect(parsed.objectValue?["a"] == .int(1))
    }

    @Test func unknownKeyHelperRejects() throws {
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(Strict.self, from: Data(#"{"field":1,"zzz":2}"#.utf8))
        }
        let ok = try JSONDecoder().decode(Strict.self, from: Data(#"{"field":1}"#.utf8))
        #expect(ok.field == 1)
    }

    @Test func serializationBridgePreservesEveryKind() throws {
        let source = Data(#"{"b":true,"i":3,"d":2.5,"s":"x","n":null,"a":[1,false]}"#.utf8)
        let any = try JSONSerialization.jsonObject(with: source)
        let value = try JSONValue.from(any: any)
        let obj = value.objectValue
        #expect(obj?["b"] == .bool(true))
        #expect(obj?["i"] == .int(3))
        #expect(obj?["d"] == .double(2.5))
        #expect(obj?["s"] == .string("x"))
        #expect(obj?["n"] == .null)
        #expect(obj?["a"] == .array([.int(1), .bool(false)]))
        let roundTripped = try JSONValue.from(any: value.anyValue)
        #expect(roundTripped == value)
    }

    @Test func integralDoublesCollapseToIntOnTheWire() throws {
        // Pinned toolchain behavior (Foundation emits shortest number form):
        // JSONEncoder drops the fraction of an integral Double, and decoding tries
        // Int64 first — so .double(2.0) does not survive a round-trip. Downstream
        // Double-typed consumers must read through `doubleValue`.
        let encoded = try JSONEncoder().encode(JSONValue.double(2.0))
        let back = try JSONDecoder().decode(JSONValue.self, from: encoded)
        #expect(back == .int(2))

        let fromLiteral = try JSONDecoder().decode(JSONValue.self, from: Data("2.0".utf8))
        #expect(fromLiteral == .int(2))
    }

    @Test func doubleValueToleratesIntegralCollapse() {
        #expect(JSONValue.double(2.5).doubleValue == 2.5)
        #expect(JSONValue.double(2.0).doubleValue == 2.0)
        #expect(JSONValue.int(3).doubleValue == 3.0)
        #expect(JSONValue.int(2).doubleValue == 2.0)
        #expect(JSONValue.string("2").doubleValue == nil)
        #expect(JSONValue.bool(true).doubleValue == nil)
        #expect(JSONValue.null.doubleValue == nil)
        #expect(JSONValue.array([]).doubleValue == nil)
    }

    @Test func serializationBridgeRejectsIntegersOutsideInt64() throws {
        #expect(throws: CocoaError.self) {
            _ = try JSONValue.from(any: UInt64.max)
        }
        // Just inside the range still bridges.
        let maxInt = try JSONValue.from(any: UInt64(Int64.max))
        #expect(maxInt == .int(Int64.max))
    }
}

private struct Strict: Codable, Equatable {
    let field: Int
    enum CodingKeys: String, CodingKey, CaseIterable { case field }

    init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("Strict", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        field = try container.decode(Int.self, forKey: .field)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(field, forKey: .field)
    }
}
