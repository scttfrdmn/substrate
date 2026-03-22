package substrate

import (
	"fmt"
	"strconv"
	"time"
)

// EpochSeconds is a time.Time that marshals to/from a JSON float64 representing
// Unix epoch seconds with millisecond precision, as required by the AWS SDK v2
// smithytime decoder (which calls ParseEpochSeconds on timestamp fields).
// UnmarshalJSON also accepts RFC3339 strings for backward-compatible state reads.
type EpochSeconds time.Time

// MarshalJSON serializes e as a JSON float64 (Unix seconds, 3 decimal places).
// A zero time marshals as JSON null.
func (e EpochSeconds) MarshalJSON() ([]byte, error) {
	t := time.Time(e)
	if t.IsZero() {
		return []byte("null"), nil
	}
	f := float64(t.UnixNano()) / 1e9
	return []byte(strconv.FormatFloat(f, 'f', 3, 64)), nil
}

// UnmarshalJSON deserializes a JSON float64 (epoch seconds) or RFC3339 string.
func (e *EpochSeconds) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	// Try float64 first (canonical wire format).
	if len(b) > 0 && (b[0] == '-' || (b[0] >= '0' && b[0] <= '9')) {
		f, err := strconv.ParseFloat(string(b), 64)
		if err == nil {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			*e = EpochSeconds(time.Unix(sec, nsec))
			return nil
		}
	}
	// Fallback: RFC3339 string (legacy persisted state).
	if len(b) >= 2 && b[0] == '"' {
		s := string(b[1 : len(b)-1])
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return fmt.Errorf("EpochSeconds: cannot parse RFC3339 %q: %w", s, err)
		}
		*e = EpochSeconds(t)
		return nil
	}
	return fmt.Errorf("EpochSeconds: cannot parse %s", b)
}

// IsZero reports whether e represents the zero time instant.
func (e EpochSeconds) IsZero() bool { return time.Time(e).IsZero() }

// Time returns the underlying time.Time value.
func (e EpochSeconds) Time() time.Time { return time.Time(e) }
