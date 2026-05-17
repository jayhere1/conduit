//! Tracing initialization for the Conduit CLI.
//!
//! Default build: installs the existing `tracing_subscriber::fmt` subscriber —
//! identical behavior to what `main` used to inline.
//!
//! `otel` feature build: if `OTEL_EXPORTER_OTLP_ENDPOINT` is set in the
//! environment, additionally installs an OTLP tracer (service name `conduit`)
//! as a layer alongside fmt so the spans emitted by `conduit-executor` and
//! `conduit-scheduler` can ship to a collector (Jaeger / Tempo / Honeycomb /
//! the OTel collector itself). If the env var is unset the OTLP layer is
//! skipped and behavior matches the default build.
//!
//! See Bet 3 in `docs/STRATEGIC_DIRECTION.md` for context — the executor and
//! scheduler already emit `tracing::info_span!` / `tracing::error!` events
//! around the task lifecycle; this module is just the exporter wiring.
//!
//! Activation is env-var driven (`OTEL_EXPORTER_OTLP_ENDPOINT`, the
//! conventional OpenTelemetry SDK env var), not a CLI flag, to keep the
//! command-surface small.

use tracing_subscriber::EnvFilter;

/// Build the env-filter from the `--verbose` flag, mirroring the prior
/// inline behavior in `main`: `debug` when verbose, otherwise `warn`.
fn build_filter(verbose: bool) -> EnvFilter {
    if verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("warn")
    }
}

/// Initialize the global tracing subscriber.
///
/// Idempotency: `tracing_subscriber::*::init` panics if called twice in the
/// same process. The CLI calls this exactly once from `main`. Tests that
/// invoke this should run in isolated processes (which `cargo test` does
/// per-test-binary; in-process test cases use `try_init` paths internally).
#[cfg(not(feature = "otel"))]
pub fn init_tracing(verbose: bool) {
    let filter = build_filter(verbose);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init();
}

#[cfg(feature = "otel")]
pub fn init_tracing(verbose: bool) {
    use opentelemetry::{global, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = build_filter(verbose);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .compact();

    // Endpoint is the standard OTel SDK env var — unset means "don't install
    // the exporter," preserving fmt-only behavior. We deliberately do not
    // expose a CLI flag for this.
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

    if let Some(endpoint) = endpoint {
        // Propagator for distributed-trace context (W3C tracecontext).
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build();

        match exporter {
            Ok(exporter) => {
                let provider = sdktrace::TracerProvider::builder()
                    .with_batch_exporter(exporter, runtime::Tokio)
                    .with_resource(Resource::new(vec![KeyValue::new(
                        "service.name",
                        "conduit",
                    )]))
                    .build();

                let tracer = {
                    use opentelemetry::trace::TracerProvider as _;
                    provider.tracer("conduit")
                };
                global::set_tracer_provider(provider);

                let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

                let _ = tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt_layer)
                    .with(otel_layer)
                    .try_init();
                return;
            }
            Err(err) => {
                // Don't abort — log to stderr and fall through to fmt-only
                // so a misconfigured collector endpoint doesn't take the CLI
                // down. The user can re-run with `--verbose` to see this in
                // tracing output if needed.
                eprintln!(
                    "conduit: OTLP exporter init failed ({err}); continuing with fmt subscriber only"
                );
            }
        }
    }

    // No endpoint configured (or exporter init failed) — fmt only.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Calling `init_tracing` once must not panic regardless of whether the
    /// `otel` feature is enabled. We use `try_init` internally so a duplicate
    /// global subscriber in another test won't blow this up either.
    #[test]
    fn init_tracing_runs_without_panic() {
        init_tracing(false);
    }

    #[test]
    fn init_tracing_verbose_runs_without_panic() {
        init_tracing(true);
    }
}
