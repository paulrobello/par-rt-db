// swift-tools-version:6.0
import PackageDescription

// ENH-031: this manifest lives at the repo root so Swift consumers can depend
//   .package(url: "https://github.com/paulrobello/par-rt-db", exact: "X.Y.Z")
//
// `exact:` takes the semantic version without the `v` prefix; SwiftPM resolves
// it against the corresponding `vX.Y.Z` git tag.
// The declared name matches the package identity SwiftPM derives from that
// URL, so products are referenced as .product(name:_, package: "par-rt-db").
// There is deliberately no nested swift-client/Package.swift: every target
// below points into swift-client/ with an explicit path: entry — one manifest,
// no dual sources of truth. swiftformat/swiftlint stay scoped to
// swift-client/ (their configs live there).
let package = Package(
    name: "par-rt-db",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "ParRtDbClient", targets: ["ParRtDbClient"]),
        .library(name: "ParRtDbUI", targets: ["ParRtDbUI"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.4.3"),
    ],
    targets: [
        .target(
            name: "ParRtDbClient",
            path: "swift-client/Sources/ParRtDbClient"
        ),
        .target(
            name: "ParRtDbUI",
            dependencies: ["ParRtDbClient"],
            path: "swift-client/Sources/ParRtDbUI"
        ),
        .testTarget(
            name: "ParRtDbClientTests",
            dependencies: ["ParRtDbClient"],
            path: "swift-client/Tests/ParRtDbClientTests"
        ),
        .testTarget(
            name: "ParRtDbUITests",
            dependencies: ["ParRtDbUI", "ParRtDbClient"],
            path: "swift-client/Tests/ParRtDbUITests"
        ),
    ]
)
