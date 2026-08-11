//! Tracing subscriber initialization (ENH-018).
//!
//! The default build writes structured logs to stdout via `tracing-subscriber`'s
//! fmt layer — identical to the pre-ENH-018 behavior. When the `otel` cargo
//! feature is compiled in AND `RTDB_OTEL_ENABLED=true`, an
//! [`OpenTelemetryLayer`] is composed on top so spans export over OTLP/gRPC to
//! a collector. The layer is the only thing the feature gates; the span
//! instrumentation in the committer/subs/query/txn modules is unconditional
//! `tracing` macros, which are no-ops when no otel layer is installed (and
//! near-zero-cost even when one is, behind the head sampler).
//!
//! Build the subscriber with [`init`] after `Config::from_env` so the runtime
//! `RTDB_OTEL_*` knobs are honored. Drop the returned guard to flush + shut
//! down the tracer provider on SIGTERM (a docker `compose down` otherwise drops
//! the last in-flight batch).

use crate::config::Config;

/// Initializes the global tracing subscriber. Returns a guard whose `Drop`
/// flushes the OTLP exporter and shuts the provider down so the last batch of
/// spans is not lost on a graceful exit. The guard is `None` when otel is off
/// (default) — there is nothing to flush.
///
/// # Panics
/// Panics if a global subscriber is already installed (it never is in normal
/// boot — `main` is the only caller). This matches the prior `init()` posture.
#[must_use = "the returned guard flushes the OTLP exporter on drop; dropping it immediately forfeits the last span batch"]
pub fn init(config: &Config) -> Option<OtelGuard> {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    #[cfg(feature = "otel")]
    {
        if config.otel_enabled {
            return install_otel(config, env_filter);
        }
    }
    // Default path (feature off, or feature on but RTDB_OTEL_ENABLED=false):
    // stdout-only, byte-compatible with pre-ENH-018 behavior. The `config` arg
    // is used only on the otel path above; reference it so the off-build does
    // not warn under `-D warnings`.
    let _ = config;
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    None
}

/// Returns the boot-time span attributes that identify this process: the
/// configured `service.name` and the version/commit `health` already exposes.
/// Kept here so every otel-init path stamps the same resource attributes.
#[cfg(feature = "otel")]
fn resource_attrs(config: &Config) -> Vec<opentelemetry::KeyValue> {
    use opentelemetry::KeyValue;
    let mut attrs = vec![KeyValue::new(
        "service.name",
        config.otel_service_name.clone(),
    )];
    // service.version mirrors what health.rs reports — the build commit, when
    // compiled with it; absent otherwise (the KV is simply omitted).
    if let Some(version) = option_env!("RTDB_BUILD_COMMIT")
        && !version.is_empty()
    {
        attrs.push(KeyValue::new("service.version", version.to_string()));
    }
    attrs
}

#[cfg(feature = "otel")]
fn install_otel(config: &Config, env_filter: tracing_subscriber::EnvFilter) -> Option<OtelGuard> {
    use opentelemetry::global;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::Sampler;
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otel_endpoint)
        .build()
    {
        Ok(exp) => exp,
        Err(err) => {
            // Fall back to stdout-only rather than aborting boot — tracing is
            // observability, not correctness. The operator sees the failure and
            // fixes the endpoint/config; the server stays up.
            tracing::error!(
                error = %err,
                endpoint = %config.otel_endpoint,
                "failed to build OTLP exporter; falling back to stdout-only tracing"
            );
            tracing_subscriber::fmt().with_env_filter(env_filter).init();
            return None;
        }
    };

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_attributes(resource_attrs(config))
                .build(),
        )
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.otel_sample_ratio,
        ))))
        .build();

    // Set the global provider BEFORE building the layer's tracer. In
    // tracing-opentelemetry 0.32 / opentelemetry 0.31, `global::tracer("…")`
    // returns a proxy that resolves to whatever provider is global *at call
    // time*; capturing it before `set_tracer_provider` leaves the layer pinned
    // to the no-op default and no spans ever export (caught during the ENH-018
    // collector e2e — the subscriber installed cleanly but the collector
    // received zero spans).
    global::set_tracer_provider(provider.clone());

    let tracer = global::tracer("par-rt-db");
    let otel_layer = OpenTelemetryLayer::new(tracer);

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer());
    if registry.try_init().is_err() {
        // A global subscriber is already installed (e.g. a test harness).
        // Keep the provider around so the guard still shuts it down cleanly.
        tracing::warn!("global tracing subscriber already set; OTLP layer not installed");
    }

    tracing::info!(
        endpoint = %config.otel_endpoint,
        ratio = config.otel_sample_ratio,
        "OTLP tracing export enabled"
    );
    Some(OtelGuard { provider })
}

/// RAII guard: on drop, flushes the OTLP exporter and shuts the provider down
/// so the last in-flight span batch reaches the collector before the process
/// exits. Created only when otel is installed. Holds a clone of the provider so
/// `Drop` can call `shutdown()` directly (0.31's global module does not expose
/// a shutdown entrypoint).
pub struct OtelGuard {
    #[cfg(feature = "otel")]
    provider: opentelemetry_sdk::trace::SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        {
            // Best-effort: a flush failure is logged, not fatal — the process
            // is already exiting.
            if let Err(err) = self.provider.shutdown() {
                tracing::warn!(error = %err, "OTLP provider shutdown reported an error");
            }
        }
    }
}
