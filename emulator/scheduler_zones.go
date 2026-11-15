package emulator

import (
	"archive/zip"
	_ "embed" // the IANA database below is embedded
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Time zones for EventBridge Scheduler, resolved from one embedded copy of the IANA database (#1514).
//
// The User Guide's schedule-types page: "EventBridge Scheduler uses the Time Zone Database maintained
// by the Internet Assigned Numbers Authority (IANA)". A ScheduleExpressionTimezone is accepted when it
// names a zone in that database, exactly — names are matched case-sensitively — and the zone's rules
// come from the same copy. time.LoadLocation is not used: it consults the host's zoneinfo before any
// embedded copy, accepts "Local" (the host's own zone), and on a case-insensitive file system accepts
// "america/new_york". Each of those would make whether a schedule is accepted, and when it fires,
// depend on the machine a run or its replay happens on.
//
// scheduler_zoneinfo.zip is Go's lib/time/zoneinfo.zip, IANA release 2026c, copied unchanged; the same
// archive time/tzdata embeds. Refresh it by copying the file from a newer Go release.

//go:embed scheduler_zoneinfo.zip
var schedZoneinfoZip string

// schedZoneData indexes the embedded archive's zone files by name. It is built once and never
// written after, so it is a cache of immutable data rather than mutable state.
var schedZoneData = sync.OnceValues(func() (map[string]*zip.File, error) { //nolint:gochecknoglobals // immutable cache of embedded data
	r, err := zip.NewReader(strings.NewReader(schedZoneinfoZip), int64(len(schedZoneinfoZip)))
	if err != nil {
		return nil, fmt.Errorf("open embedded zoneinfo: %w", err)
	}
	files := make(map[string]*zip.File, len(r.File))
	for _, f := range r.File {
		files[f.Name] = f
	}
	return files, nil
})

// errSchedUnknownZone reports a ScheduleExpressionTimezone the IANA database does not name.
var errSchedUnknownZone = fmt.Errorf("%w: not an IANA time zone", errSchedExpression)

// schedLocation resolves a ScheduleExpressionTimezone, UTC when the schedule names none. It is the one
// resolver both the CreateSchedule/UpdateSchedule validation and the clock's evaluation use, so what
// is accepted is exactly what can be evaluated.
func schedLocation(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}
	files, err := schedZoneData()
	if err != nil {
		return nil, err
	}
	f, ok := files[tz]
	if !ok {
		return nil, fmt.Errorf("%w: %q", errSchedUnknownZone, tz)
	}
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("open zone %q: %w", tz, err)
	}
	data, err := io.ReadAll(rc)
	if closeErr := rc.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("read zone %q: %w", tz, err)
	}
	loc, err := time.LoadLocationFromTZData(tz, data)
	if err != nil {
		return nil, fmt.Errorf("%w: zone %q: %w", errSchedExpression, tz, err)
	}
	return loc, nil
}
