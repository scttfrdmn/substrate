package substrate

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracingConfig holds OpenTelemetry distributed tracing configuration.
type TracingConfig struct {
	// Enabled gates tracing. When false, a noop tracer is returned.
	Enabled bool

	// Exporter selects the trace exporter: "noop", "stdout", or "otlp_http".
	// Defaults to "noop".
	Exporter string

	// OTLPEndpoint is the OTLP/HTTP collector endpoint (e.g.
	// "http://localhost:4318"). Required when Exporter is "otlp_http".
	OTLPEndpoint string

	// ServiceName is the OpenTelemetry service.name resource attribute.
	// Defaults to "substrate".
	ServiceName string
}

// Tracer wraps an OpenTelemetry [trace.Tracer] with a substrate-specific API.
type Tracer struct {
	inner trace.Tracer
}

// NewTracer creates a configured [Tracer] and returns a shutdown function that
// flushes all pending spans. When cfg.Enabled is false or
// cfg.Exporter is "noop", a no-op tracer is returned with a no-op shutdown.
func NewTracer(ctx context.Context, cfg TracingConfig) (*Tracer, func(context.Context) error, error) {
	if !cfg.Enabled || cfg.Exporter == "" || cfg.Exporter == "noop" {
		t := &Tracer{inner: noop.NewTracerProvider().Tracer("substrate")}
		return t, func(context.Context) error { return nil }, nil
	}

	svcName := cfg.ServiceName
	if svcName == "" {
		svcName = "substrate"
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(svcName),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build otel resource: %w", err)
	}

	var exp sdktrace.SpanExporter
	switch cfg.Exporter {
	case "stdout":
		exp, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, nil, fmt.Errorf("create stdout exporter: %w", err)
		}
	case "otlp_http":
		if cfg.OTLPEndpoint == "" {
			return nil, nil, fmt.Errorf("tracing.otlp_endpoint must be set when exporter is otlp_http")
		}
		exp, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpointURL(cfg.OTLPEndpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("create otlp_http exporter: %w", err)
		}
	default:
		return nil, nil, fmt.Errorf("unknown tracing exporter %q; choose noop, stdout, or otlp_http", cfg.Exporter)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	shutdown := func(shutCtx context.Context) error {
		if shutErr := tp.Shutdown(shutCtx); shutErr != nil {
			return fmt.Errorf("shutdown tracer provider: %w", shutErr)
		}
		return nil
	}

	t := &Tracer{inner: tp.Tracer("substrate")}
	return t, shutdown, nil
}

// StartSpan creates a new child span with the given name and optional
// attributes. The caller must call span.End() when the operation completes.
func (t *Tracer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{}
	if len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(attrs...))
	}
	return t.inner.Start(ctx, name, opts...)
}

// StartRequest starts a "substrate.request" span enriched with the AWS service
// and operation names. The caller is responsible for calling span.End() when
// the request completes, typically via defer.
func (t *Tracer) StartRequest(ctx context.Context, service, operation string) (context.Context, trace.Span) {
	return t.StartSpan(ctx, "substrate.request",
		attribute.String("aws.service", service),
		attribute.String("aws.operation", operation),
	)
}

// RecordSpanError marks span as errored with the given error and sets the span
// status to codes.Error. It is safe to call with a nil span.
func RecordSpanError(span trace.Span, err error) {
	if span == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
