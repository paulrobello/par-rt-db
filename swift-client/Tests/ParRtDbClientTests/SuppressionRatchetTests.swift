import Foundation
import Testing

// QA-002 ratchet: the Swift client mirrors the server's interpreter engines by
// hand, and where a mirror grows past SwiftLint's complexity limits it
// suppresses with in-source disable directives instead of decomposing to match
// the server's shape. The wire corpus pins behavior but not structure, so
// nothing else notices structural rot. This test counts the disable directives
// under swift-client/Sources and fails when the count grows past the baseline:
// decompose the mirrored function instead of adding a directive (the baseline
// may only move DOWN, with a dated note when a cleanup lowers it).

/// One unit per source line carrying a disable directive, in any form
/// (region, `:next_line`, `:this_line`, `:previous_line`). Recorded
/// 2026-11-04. Move it down when cleanups land; never up.
private let suppressionBaseline = 48

/// Marker text shared by every disable directive form.
private let disableMarker = "swiftlint:disable"

private enum RatchetError: Error, CustomStringConvertible {
    case noPackageManifest(String)
    case noSourcesFound(String)

    var description: String {
        switch self {
        case let .noPackageManifest(path):
            "Package.swift not found at \(path) — #filePath moved or the root manifest was relocated"
        case let .noSourcesFound(path):
            "no .swift files under \(path) — the ratchet scanned nothing and would pass vacuously"
        }
    }
}

/// Repo root derived from this file's compile-time path: four components up
/// from <root>/swift-client/Tests/ParRtDbClientTests/SuppressionRatchetTests.swift.
/// The Package.swift check fails loudly if the layout moves instead of
/// silently scanning nothing.
private func repoRoot() throws -> URL {
    let root = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent() // ParRtDbClientTests
        .deletingLastPathComponent() // Tests
        .deletingLastPathComponent() // swift-client
        .deletingLastPathComponent() // repo root
    let manifest = root.appendingPathComponent("Package.swift").path
    guard FileManager.default.fileExists(atPath: manifest) else {
        throw RatchetError.noPackageManifest(root.path)
    }
    return root
}

/// Result of the Sources scan: the directive-line total, the per-file
/// breakdown (non-zero files only, sorted by name) for failure output, and
/// the number of Swift files scanned.
private struct ScanResult {
    var total: Int
    var perFile: [(file: String, count: Int)]
    var scanned: Int
}

/// Counts disable-directive lines under Sources.
private func countDisableDirectives(sources: URL) throws -> ScanResult {
    var swiftFiles: [URL] = []
    let enumerator = FileManager.default.enumerator(
        at: sources,
        includingPropertiesForKeys: [.isDirectoryKey],
        options: [.skipsHiddenFiles]
    )
    while let item = enumerator?.nextObject() as? URL {
        let values = try item.resourceValues(forKeys: [.isDirectoryKey])
        if values.isDirectory != true, item.pathExtension == "swift" {
            swiftFiles.append(item)
        }
    }
    guard !swiftFiles.isEmpty else {
        throw RatchetError.noSourcesFound(sources.path)
    }
    swiftFiles.sort { $0.lastPathComponent < $1.lastPathComponent }
    var perFile: [(file: String, count: Int)] = []
    for file in swiftFiles {
        let contents = try String(contentsOf: file, encoding: .utf8)
        let hits = contents
            .components(separatedBy: .newlines)
            .filter { $0.contains(disableMarker) }
            .count
        if hits > 0 {
            perFile.append((file.lastPathComponent, hits))
        }
    }
    return ScanResult(
        total: perFile.reduce(0) { $0 + $1.count },
        perFile: perFile,
        scanned: swiftFiles.count
    )
}

struct SuppressionRatchetTests {
    @Test
    func swiftLintDisableDirectiveCountDoesNotGrow() throws {
        let sources = try repoRoot().appendingPathComponent("swift-client/Sources")
        let result = try countDisableDirectives(sources: sources)

        // Guards the scan itself: a wrong root or an empty tree must fail,
        // not pass the ratchet by counting zero directives. Sources held 37
        // Swift files at the 2026-11-04 baseline; the floor only needs to be
        // far enough below that to survive ordinary file churn.
        #expect(
            result.scanned >= 30,
            "scanned only \(result.scanned) Swift files under Sources — expected the whole tree"
        )

        let breakdown = result.perFile
            .map { "\($0.count)x \($0.file)" }
            .joined(separator: ", ")
        #expect(
            result.total <= suppressionBaseline,
            """
            swiftlint disable directives in Sources grew to \(result.total) \
            (baseline \(suppressionBaseline), recorded 2026-11-04). Decompose \
            the mirrored function to the server's shape instead of \
            suppressing. Current breakdown: \(breakdown)
            """
        )
    }
}
