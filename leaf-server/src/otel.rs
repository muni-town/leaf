use std::sync::{Arc, LazyLock};

use pyroscope::{PyroscopeAgent, backend::Tag, pyroscope::PyroscopeAgentRunning};

use opentelemetry::{
    KeyValue,
    global::{self, ObjectSafeSpan},
    trace::TracerProvider as _,
};
use opentelemetry_sdk::{
    Resource,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider, SpanProcessor},
};
use opentelemetry_semantic_conventions::{SCHEMA_URL, attribute::SERVICE_VERSION};
use pyroscope_pprofrs::{PprofConfig, pprof_backend};
use rand::{Rng, rng};
use tracing::Level;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt};

use crate::ARGS;

/// Process ID used as a profile ID for pyroscope
static PROCESS_ID: LazyLock<String> = LazyLock::new(|| {
    let bytes = rng().random::<[u8; 8]>();
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
});

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
fn init_tracer_provider(enable_profiling: bool) -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .unwrap();

    #[derive(Debug)]
    struct ProfileLinkProcessor;
    impl SpanProcessor for ProfileLinkProcessor {
        fn on_start(
            &self,
            span: &mut opentelemetry_sdk::trace::Span,
            _cx: &opentelemetry::Context,
        ) {
            span.set_attribute(KeyValue::new("pyroscope.profile.id", PROCESS_ID.clone()));
        }
        fn on_end(&self, _span: opentelemetry_sdk::trace::SpanData) {}
        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(
            &self,
            _timeout: std::time::Duration,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
    }

    let mut provider_builder = SdkTracerProvider::builder()
        // Customize sampling strategy
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        // If export trace to AWS X-Ray, you can use XrayIdGenerator
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource())
        .with_batch_exporter(exporter);
    if enable_profiling {
        provider_builder = provider_builder.with_span_processor(ProfileLinkProcessor);
    }
    let provider = provider_builder.build();

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
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(filter_fn(|meta| !meta.target().starts_with("Pyroscope"))),
        );

    let profiler = if ARGS.profiling {
        let agent = pyroscope::PyroscopeAgent::builder("http://localhost:4040", "leaf-server")
            .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
            .build()
            .expect("Init profiler")
            .start()
            .expect("Start profiling agent");
        agent
            .add_global_tag(Tag::new("span_id".into(), PROCESS_ID.clone()))
            .unwrap();
        Some(agent)
    } else {
        None
    };

    let (tracer_provider, meter_provider) = if ARGS.otel {
        let tracer_provider = Arc::new(init_tracer_provider(ARGS.profiling));
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
        profiler,
    }
}

pub struct OtelGuard {
    pub tracer_provider: Option<Arc<SdkTracerProvider>>,
    pub meter_provider: Option<SdkMeterProvider>,
    pub profiler: Option<PyroscopeAgent<PyroscopeAgentRunning>>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(Err(err)) = self.tracer_provider.take().map(|x| x.shutdown()) {
            eprintln!("{err:?}");
        }
        if let Some(Err(err)) = self.meter_provider.take().map(|x| x.shutdown()) {
            eprintln!("{err:?}");
        }
        match self.profiler.take().map(|x| x.stop()) {
            Some(Ok(profiler)) => profiler.shutdown(),
            Some(Err(e)) => eprintln!("Error stopping profiler: {e}"),
            None => (),
        }
    }
}
