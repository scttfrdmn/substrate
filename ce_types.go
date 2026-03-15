package substrate

// ceNamespace is the service name used by CEPlugin.
const ceNamespace = "ce"

// CECostResultByTime is a single time-period bucket returned by GetCostAndUsage.
type CECostResultByTime struct {
	// TimePeriod is the date range this bucket covers.
	TimePeriod CEDateInterval `json:"TimePeriod"`

	// Total maps metric names (e.g. "UnblendedCost") to their aggregated values.
	Total map[string]CEMetric `json:"Total"`

	// Groups holds per-dimension breakdowns when GroupBy is specified.
	Groups []CEGroup `json:"Groups,omitempty"`

	// Estimated indicates whether this period's data is estimated.
	Estimated bool `json:"Estimated"`
}

// CEDateInterval represents a half-open date range [Start, End).
type CEDateInterval struct {
	// Start is the start date in YYYY-MM-DD format (inclusive).
	Start string `json:"Start"`

	// End is the end date in YYYY-MM-DD format (exclusive).
	End string `json:"End"`
}

// CEMetric holds a cost or usage amount and its unit.
type CEMetric struct {
	// Amount is the decimal string value (e.g. "1.234500").
	Amount string `json:"Amount"`

	// Unit is the currency or unit string (e.g. "USD").
	Unit string `json:"Unit"`
}

// CEGroup is a cost breakdown group returned when GroupBy is specified.
type CEGroup struct {
	// Keys holds the dimension values for this group (e.g. service name).
	Keys []string `json:"Keys"`

	// Metrics maps metric names to their values for this group.
	Metrics map[string]CEMetric `json:"Metrics"`
}
