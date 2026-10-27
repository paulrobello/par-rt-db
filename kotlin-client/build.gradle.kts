plugins {
    kotlin("jvm") version "2.4.21"
    kotlin("plugin.serialization") version "2.4.21"
}

group = "dev.parrtdb"
version = "0.1.0"

repositories { mavenCentral() }

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    // Optional: only OkHttpTransport needs it; Android apps add okhttp themselves.
    compileOnly("com.squareup.okhttp3:okhttp:4.12.0")
    testImplementation("com.squareup.okhttp3:okhttp:4.12.0")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
}

// Bytecode target 17 keeps the artifact consumable from Android (AGP 8+) and any
// JDK 17+ host; the build itself runs on whatever JDK is installed.
kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
        allWarningsAsErrors.set(true)
    }
}
java { sourceCompatibility = JavaVersion.VERSION_17; targetCompatibility = JavaVersion.VERSION_17 }

tasks.test {
    useJUnitPlatform()
    testLogging { events("failed", "skipped"); showStandardStreams = false }
}
