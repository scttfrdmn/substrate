package substrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// injectDailyCosts injects one event per day starting daysAgo days before now,
// each with the specified cost, into store.
func injectDailyCosts(t *testing.T, store *substrate.EventStore, service string, costPerDay float64, daysAgo int) {
	t.Helper()
	base := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -daysAgo)
	for d := 0; d < daysAgo; d++ {
		ts := base.Add(time.Duration(d) * 24 * time.Hour).Add(time.Hour)
		ev := &substrate.Event{
			Timestamp: ts,
			AccountID: "123456789012",
			Service:   service,
			Operation: "PutObject",
			Cost:      costPerDay,
		}
		require.NoError(t, substrate.RecordEventAtTimeForTest(store, ev))
	}
}

func TestGetCostForecast_EmptyStore(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	fc, err := store.GetCostForecast(context.Background(), "", "", 30, 7, 2.0)
	require.NoError(t, err)
	assert.Equal(t, 0.0, fc.ProjectedCost)
	assert.Empty(t, fc.DailyCosts)
	assert.Empty(t, fc.Anomalies)
}

func TestGetCostForecast_LinearTrend(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	injectDailyCosts(t, store, "s3", 1.0, 14)

	fc, err := store.GetCostForecast(context.Background(), "", "", 30, 7, 2.0)
	require.NoError(t, err)
	require.NotNil(t, fc)
	assert.Equal(t, 30, fc.WindowDays)
	assert.Equal(t, 7, fc.HorizonDays)
	// Flat $1/day → projected ~$7 over 7 days (within 20%).
	assert.InDelta(t, 7.0, fc.ProjectedCost, 2.0)
	assert.GreaterOrEqual(t, fc.ConfidenceHigh, fc.ProjectedCost)
	assert.LessOrEqual(t, fc.ConfidenceLow, fc.ProjectedCost)
}

func TestGetCostForecast_AnomalyDetection(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	// 13 days at $1/day, then 1 day spike at $100 (today - 1 day = yesterday).
	base := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -14)
	for d := 0; d < 13; d++ {
		ts := base.Add(time.Duration(d) * 24 * time.Hour).Add(time.Hour)
		ev := &substrate.Event{
			Timestamp: ts,
			AccountID: "123456789012",
			Service:   "lambda",
			Operation: "Invoke",
			Cost:      1.0,
		}
		require.NoError(t, substrate.RecordEventAtTimeForTest(store, ev))
	}
	// Spike on the most recent day inside the window.
	spikeDay := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1).Add(time.Hour)
	require.NoError(t, substrate.RecordEventAtTimeForTest(store, &substrate.Event{
		Timestamp: spikeDay,
		AccountID: "123456789012",
		Service:   "lambda",
		Operation: "Invoke",
		Cost:      100.0,
	}))

	fc, err := store.GetCostForecast(context.Background(), "", "lambda", 30, 7, 2.0)
	require.NoError(t, err)
	require.NotEmpty(t, fc.Anomalies, "expected at least one anomaly for the spike day")
	assert.Equal(t, "lambda", fc.Anomalies[0].Service)
	assert.Greater(t, fc.Anomalies[0].SigmaCount, 2.0)
}

func TestGetCostForecast_InsufficientData(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	// Only 2 events on the same day — insufficient for regression (< 3 days).
	for i := 0; i < 2; i++ {
		ev := &substrate.Event{
			Timestamp: time.Now().UTC(),
			AccountID: "123456789012",
			Service:   "sqs",
			Operation: "SendMessage",
			Cost:      0.5,
		}
		require.NoError(t, substrate.RecordEventAtTimeForTest(store, ev))
	}

	fc, err := store.GetCostForecast(context.Background(), "", "", 30, 7, 2.0)
	require.NoError(t, err)
	// Fallback to mean: total cost on one day = 1.0, mean = 1.0, projected = 1.0 * 7 = 7.0.
	assert.Equal(t, fc.ProjectedCost, fc.ConfidenceLow)
	assert.Equal(t, fc.ProjectedCost, fc.ConfidenceHigh)
}

func TestGetCostForecast_Defaults(t *testing.T) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	fc, err := store.GetCostForecast(context.Background(), "", "", 0, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, 30, fc.WindowDays)
	assert.Equal(t, 7, fc.HorizonDays)
}

func TestLinearRegression(t *testing.T) {
	// y = 2x + 1 → slope=2, intercept=1.
	xs := []float64{0, 1, 2, 3, 4}
	ys := []float64{1, 3, 5, 7, 9}
	slope, intercept := substrate.LinearRegressionForTest(xs, ys)
	assert.InDelta(t, 2.0, slope, 0.001)
	assert.InDelta(t, 1.0, intercept, 0.001)
}

func TestLinearRegression_Empty(t *testing.T) {
	slope, intercept := substrate.LinearRegressionForTest(nil, nil)
	assert.Equal(t, 0.0, slope)
	assert.Equal(t, 0.0, intercept)
}

func TestLinearRegression_ConstantX(t *testing.T) {
	// All xs equal → denominator zero → returns mean y.
	xs := []float64{1, 1, 1}
	ys := []float64{2, 4, 6}
	slope, intercept := substrate.LinearRegressionForTest(xs, ys)
	assert.Equal(t, 0.0, slope)
	assert.InDelta(t, 4.0, intercept, 0.001)
}

func TestMeanFloat(t *testing.T) {
	assert.Equal(t, 0.0, substrate.MeanFloatForTest(nil))
	assert.InDelta(t, 2.0, substrate.MeanFloatForTest([]float64{1, 2, 3}), 0.001)
}

func TestStddevFloat(t *testing.T) {
	assert.Equal(t, 0.0, substrate.StddevFloatForTest([]float64{5}, 5))
	vals := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	mean := substrate.MeanFloatForTest(vals)
	sd := substrate.StddevFloatForTest(vals, mean)
	assert.InDelta(t, 2.138, sd, 0.01)
}
