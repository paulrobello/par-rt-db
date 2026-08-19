// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "swift-client",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "ParRtDbClient", targets: ["ParRtDbClient"]),
        .library(name: "ParRtDbUI", targets: ["ParRtDbUI"]),
    ],
    targets: [
        .target(name: "ParRtDbClient"),
        .target(name: "ParRtDbUI", dependencies: ["ParRtDbClient"]),
        .testTarget(name: "ParRtDbClientTests", dependencies: ["ParRtDbClient"]),
        .testTarget(name: "ParRtDbUITests", dependencies: ["ParRtDbUI", "ParRtDbClient"]),
    ]
)
