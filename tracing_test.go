package substrate_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestNewTracer_Noop(t *testing.T) {
	tracer, shutdown, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:  false,
		Exporter: "noop",
	})
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestNewTracer_NoopWhenDisabled(t *testing.T) {
	tracer, shutdown, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled: false,
	})
	require.NoError(t, err)
	require.NotNil(t, tracer)
	assert.NoError(t, shutdown(context.Background()))
}

func TestNewTracer_Stdout(t *testing.T) {
	tracer, shutdown, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:     true,
		Exporter:    "stdout",
		ServiceName: "substrate-test",
	})
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestNewTracer_UnknownExporter(t *testing.T) {
	_, _, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:  true,
		Exporter: "invalid_exporter",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown tracing exporter")
}

func TestTracer_StartSpan(t *testing.T) {
	tracer, shutdown, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:  false,
		Exporter: "noop",
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	ctx, span := tracer.StartSpan(context.Background(), "test.span")
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
	span.End()
}

func TestNewTracer_OTLPMissingEndpoint(t *testing.T) {
	_, _, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:  true,
		Exporter: "otlp_http",
		// OTLPEndpoint intentionally omitted.
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "otlp_endpoint")
}

func TestTracer_StartRequest_ReturnsSpan(t *testing.T) {
	tracer, shutdown, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:  false,
		Exporter: "noop",
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	ctx, span := tracer.StartRequest(context.Background(), "s3", "PutObject")
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
	span.End()
}

func TestRecordSpanError_NilSpan(_ *testing.T) {
	// Must not panic when span is nil.
	substrate.RecordSpanError(nil, fmt.Errorf("test error"))
}

func TestRecordSpanError_NoopSpan(t *testing.T) {
	tracer, shutdown, err := substrate.NewTracer(context.Background(), substrate.TracingConfig{
		Enabled:  false,
		Exporter: "noop",
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	_, span := tracer.StartRequest(context.Background(), "s3", "GetObject")
	// Must not panic on a noop span.
	substrate.RecordSpanError(span, fmt.Errorf("NoSuchKey"))
	span.End()
}
