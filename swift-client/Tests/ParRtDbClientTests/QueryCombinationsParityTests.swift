import Foundation
@testable import ParRtDbClient
import Testing

// Guards `QueryCombinations.swift`'s embedded transcription of
// `wire-corpus/query-combinations.json` against drift — the swift package has no
// resource-bundling mechanism (unlike the semantics/golden-vector corpora, swift
// engine ships as a source-only SPM library consumers vendor without the repo's
// `wire-corpus/` directory present), so the table is a hand-generated Swift literal
// rather than a runtime-loaded resource. This test decodes the real JSON file
// (repo root, located the same way `SemanticsCorpusTests.swift`'s `corpusDirectory()`
// does) and asserts it is byte-for-byte equal — same clauses, same rule ids in the
// same order, same `forbid`/`atMostOne` membership, same code — to
// `QueryCombinationRules`. A mismatch here means the JSON changed and the Swift
// transcription was not regenerated.

private struct QueryCombinationsFile: Codable {
    let clauses: [String]
    let rules: [QueryCombinationRule]
}

/// `wire-corpus/query-combinations.json` (repo root), located from this file the
/// same way `SemanticsCorpusTests.corpusDirectory()` locates the semantics corpus.
private func queryCombinationsFileURL() -> URL {
    URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent() // ParRtDbClientTests
        .deletingLastPathComponent() // Tests
        .deletingLastPathComponent() // swift-client
        .deletingLastPathComponent() // repo root
        .appendingPathComponent("wire-corpus/query-combinations.json")
}

@Suite("Query combination rules — Swift transcription parity")
struct QueryCombinationsParityTests {
    @Test("embedded QueryCombinationRules matches wire-corpus/query-combinations.json")
    func matchesSourceOfTruth() throws {
        let data = try Data(contentsOf: queryCombinationsFileURL())
        let decoded = try JSONDecoder().decode(QueryCombinationsFile.self, from: data)

        #expect(decoded.clauses == QueryCombinationRules.clauses)
        #expect(decoded.rules == QueryCombinationRules.rules)
        #expect(decoded.rules.map(\.id) == QueryCombinationRules.rules.map(\.id))
    }

    @Test("every rule id has a matching wire-corpus/semantics/query-combo-<id>.json case")
    func everyRuleHasACorpusCase() throws {
        let semanticsDir = queryCombinationsFileURL()
            .deletingLastPathComponent() // wire-corpus
            .appendingPathComponent("semantics")
        let existing = try Set(
            FileManager.default.contentsOfDirectory(
                at: semanticsDir, includingPropertiesForKeys: nil
            )
            .map { $0.deletingPathExtension().lastPathComponent }
        )
        for rule in QueryCombinationRules.rules {
            #expect(
                existing.contains("query-combo-\(rule.id)"),
                "no corpus case for rule '\(rule.id)'"
            )
        }
    }
}
