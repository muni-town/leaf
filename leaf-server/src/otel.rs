use std::sync::Arc;

use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{SCHEMA_URL, attribute::SERVICE_VERSION};
use tracing::Level;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::ARGS;

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource() -> Resource {
    Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_schema_url(
            [KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION"))],
            SCHEMA_URL,
        )
        .build()
}

// Construct MeterProvider for MetricsLayer
fn init_meter_provider() -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
        .unwrap();

    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(30))
        .build();

    // // For debugging in development
    // let stdout_reader =
    //     PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource())
        .with_reader(reader)
        // .with_reader(stdout_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

// Construct TracerProvider for OpenTelemetryLayer
fn init_tracer_provider() -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .unwrap();

    let provider = SdkTracerProvider::builder()
        // Customize sampling strategy
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        // If export trace to AWS X-Ray, you can use XrayIdGenerator
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource())
        .with_batch_exporter(exporter)
        .build();

    global::set_tracer_provider(provider.clone());

    provider
}

// Initialize tracing-subscriber and return OtelGuard for opentelemetry-related termination processing
pub fn init() -> OtelGuard {
    let registry = tracing_subscriber::registry()
        // The global level filter prevents the exporter network stack
        // from reentering the globally installed OpenTelemetryLayer with
        // its own spans while exporting, as the libraries should not use
        // tracing levels below DEBUG. If the OpenTelemetry layer needs to
        // trace spans and events with higher verbosity levels, consider using
        // per-layer filtering to target the telemetry layer specifically,
        // e.g. by target matching.
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::INFO,
        ))
        .with(tracing_subscriber::fmt::layer());

    let (tracer_provider, meter_provider) = if ARGS.otel {
        let tracer_provider = Arc::new(init_tracer_provider());
        let meter_provider = init_meter_provider();
        let tracer = tracer_provider.tracer("tracing-otel-subscriber");

        registry
            .with(MetricsLayer::new(meter_provider.clone()))
            .with(OpenTelemetryLayer::new(tracer))
            .init();

        (Some(tracer_provider), Some(meter_provider))
    } else {
        registry.init();
        (None, None)
    };

    OtelGuard {
        tracer_provider,
        meter_provider,
    }
}

pub struct OtelGuard {
    pub tracer_provider: Option<Arc<SdkTracerProvider>>,
    pub meter_provider: Option<SdkMeterProvider>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(Err(err)) = self.tracer_provider.take().map(|x| x.shutdown()) {
            eprintln!("{err:?}");
        }
        if let Some(Err(err)) = self.meter_provider.take().map(|x| x.shutdown()) {
            eprintln!("{err:?}");
        }
    }
}
