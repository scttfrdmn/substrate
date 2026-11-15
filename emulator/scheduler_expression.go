package emulator

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	// The IANA database is embedded so a ScheduleExpressionTimezone resolves the same way on
	// every host, including a container image with no /usr/share/zoneinfo. A schedule's
	// occurrences are part of what a replay must reproduce, and a zone that loads on one machine
	// and not another would make them depend on where the replay runs (#1501).
	_ "time/tzdata"
)

// The three schedule expressions EventBridge Scheduler publishes, evaluated on the simulated
// clock (#1501).
//
// `API_CreateSchedule` publishes the forms: `at(yyyy-mm-ddThh:mm:ss)`, `rate(value unit)` with a
// positive integer and a unit of minute | minutes | hour | hours | day | days, and
// `cron(minutes hours day_of_month month day_of_week year)`. The User Guide's
// `schedule-types.html` publishes the rest of what is evaluated here:
//
//   - A one-time schedule is evaluated in ScheduleExpressionTimezone, and "EventBridge Scheduler
//     ignores the StartDate and EndDate you specify" for it.
//   - A rate schedule "starts after the start date you specify", and "If you do not provide a
//     StartDate for a rate-based schedule, your schedule starts invoking the target immediately."
//     `days` "represents a 24-hour duration", so a rate is a fixed duration and no time zone
//     applies to it.
//   - A cron schedule is evaluated in ScheduleExpressionTimezone with the published wildcards
//     (`, - * / ? L W #`). On a spring-forward day "if a cron expression falls on a non-existent
//     date and time, your schedule invocation is skipped"; on a fall-back day it "runs only once",
//     at the second occurrence of the repeated wall-clock time.
//
// Substrate fires each occurrence at its instant exactly. The guide's 60-second precision and a
// FLEXIBLE window's spread are both nondeterministic by nature, so a fired occurrence is the
// earliest moment either would allow — the window's start.

// schedExprKind names which of the three expression forms a schedule uses.
type schedExprKind int

const (
	schedExprAt schedExprKind = iota + 1
	schedExprRate
	schedExprCron
)

// schedExpression is a parsed ScheduleExpression.
type schedExpression struct {
	kind  schedExprKind
	at    time.Time
	every time.Duration
	cron  *schedCron
	loc   *time.Location
}

// errSchedExpression reports an expression substrate cannot evaluate. Such a schedule is stored
// and reported as sent, and never fires.
var errSchedExpression = errors.New("unparseable schedule expression")

// schedLocation resolves a ScheduleExpressionTimezone, UTC when the schedule names none.
func schedLocation(tz string) (*time.Location, error) {
	if tz == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("%w: timezone %q: %w", errSchedExpression, tz, err)
	}
	return loc, nil
}

// parseSchedExpression parses expr, evaluated in tz.
func parseSchedExpression(expr, tz string) (*schedExpression, error) {
	loc, err := schedLocation(tz)
	if err != nil {
		return nil, err
	}
	expr = strings.TrimSpace(expr)
	inner := func(prefix string) (string, bool) {
		if !strings.HasPrefix(expr, prefix+"(") || !strings.HasSuffix(expr, ")") {
			return "", false
		}
		return strings.TrimSpace(expr[len(prefix)+1 : len(expr)-1]), true
	}
	if body, ok := inner("at"); ok {
		t, err := time.ParseInLocation("2006-01-02T15:04:05", body, loc)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w", errSchedExpression, expr, err)
		}
		return &schedExpression{kind: schedExprAt, at: t, loc: loc}, nil
	}
	if body, ok := inner("rate"); ok {
		d, err := parseSchedRate(body)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w", errSchedExpression, expr, err)
		}
		return &schedExpression{kind: schedExprRate, every: d, loc: loc}, nil
	}
	if body, ok := inner("cron"); ok {
		c, err := parseSchedCron(body)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w", errSchedExpression, expr, err)
		}
		return &schedExpression{kind: schedExprCron, cron: c, loc: loc}, nil
	}
	return nil, fmt.Errorf("%w: %q", errSchedExpression, expr)
}

// parseSchedRate parses the body of a rate expression into its period.
func parseSchedRate(body string) (time.Duration, error) {
	fields := strings.Fields(body)
	if len(fields) != 2 {
		return 0, errors.New("a rate is a value and a unit")
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("value %q is not a positive integer", fields[0])
	}
	var unit time.Duration
	switch fields[1] {
	case "minute", "minutes":
		unit = time.Minute
	case "hour", "hours":
		unit = time.Hour
	case "day", "days":
		unit = 24 * time.Hour
	default:
		return 0, fmt.Errorf("unit %q is not minute(s), hour(s) or day(s)", fields[1])
	}
	return time.Duration(n) * unit, nil
}

// first returns the schedule's first occurrence for a configuration that took effect at armed,
// or nil when it has none. start and end are the schedule's StartDate and EndDate.
func (e *schedExpression) first(armed time.Time, start, end *time.Time) *time.Time {
	switch e.kind {
	case schedExprAt:
		// StartDate and EndDate are ignored for a one-time schedule, as the guide states. An
		// instant already past when the schedule is armed is due at once: the troubleshooting
		// guide states a one-time schedule re-enabled after its time "may immediately invoke its
		// target", and substrate reads that as the deterministic answer.
		t := e.at
		return &t
	case schedExprRate:
		anchor := armed
		if start != nil {
			anchor = *start
		}
		t := anchor
		if t.Before(armed) {
			steps := armed.Sub(anchor) / e.every
			t = anchor.Add(steps * e.every)
			if t.Before(armed) {
				t = t.Add(e.every)
			}
		}
		return clipSchedEnd(&t, end)
	case schedExprCron:
		from := armed
		if start != nil && start.After(from) {
			from = *start
		}
		return clipSchedEnd(e.cron.atOrAfter(from, e.loc), end)
	}
	return nil
}

// next returns the occurrence after prev, or nil when there is none.
func (e *schedExpression) next(prev time.Time, end *time.Time) *time.Time {
	switch e.kind {
	case schedExprRate:
		t := prev.Add(e.every)
		return clipSchedEnd(&t, end)
	case schedExprCron:
		return clipSchedEnd(e.cron.atOrAfter(prev.Add(time.Minute), e.loc), end)
	}
	return nil
}

// clipSchedEnd drops an occurrence at or after EndDate, "the date, in UTC, before which the
// schedule can invoke its target".
func clipSchedEnd(t, end *time.Time) *time.Time {
	if t == nil || (end != nil && !t.Before(*end)) {
		return nil
	}
	return t
}

// schedEpochTime converts an epoch-seconds StartDate or EndDate to a time, nil when absent.
func schedEpochTime(v *float64) *time.Time {
	if v == nil {
		return nil
	}
	sec := int64(*v)
	nsec := int64((*v - float64(sec)) * float64(time.Second))
	t := time.Unix(sec, nsec).UTC()
	return &t
}

// --- cron --------------------------------------------------------------------

// schedCronMaxYear is the last year the published Year field accepts.
const schedCronMaxYear = 2199

// schedCron is a parsed six-field cron expression.
type schedCron struct {
	minutes, hours, months, years map[int]bool
	dom                           schedCronDay
	dow                           schedCronDay
}

// schedCronDay is a parsed day-of-month or day-of-week field.
type schedCronDay struct {
	any     bool         // "?"
	set     map[int]bool // explicit values
	last    bool         // "L": the last day of the month, or Saturday in day-of-week
	weekday int          // "nW": the weekday nearest day n; 0 when unused
	lastDow int          // "nL" in day-of-week: the last such weekday of the month; 0 when unused
	nthDow  [2]int       // "d#n" in day-of-week: the nth weekday d; zero when unused
}

var schedCronMonthNames = map[string]int{ //nolint:gochecknoglobals // immutable lookup table
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var schedCronDayNames = map[string]int{ //nolint:gochecknoglobals // immutable lookup table
	"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7,
}

// parseSchedCron parses the six fields of a cron expression.
func parseSchedCron(body string) (*schedCron, error) {
	f := strings.Fields(body)
	if len(f) != 6 {
		return nil, fmt.Errorf("a cron expression has six fields, not %d", len(f))
	}
	c := &schedCron{}
	var err error
	if c.minutes, err = parseSchedCronField(f[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("minutes: %w", err)
	}
	if c.hours, err = parseSchedCronField(f[1], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("hours: %w", err)
	}
	if c.dom, err = parseSchedCronDom(f[2]); err != nil {
		return nil, fmt.Errorf("day-of-month: %w", err)
	}
	if c.months, err = parseSchedCronField(f[3], 1, 12, schedCronMonthNames); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	if c.dow, err = parseSchedCronDow(f[4]); err != nil {
		return nil, fmt.Errorf("day-of-week: %w", err)
	}
	if c.years, err = parseSchedCronField(f[5], 1970, schedCronMaxYear, nil); err != nil {
		return nil, fmt.Errorf("year: %w", err)
	}
	// "You can't use * in both the Day-of-month and Day-of-week fields. If you use it in one, you
	// must use ? in the other."
	if c.dom.any == c.dow.any {
		return nil, errors.New("exactly one of day-of-month and day-of-week must be ?")
	}
	return c, nil
}

// parseSchedCronField parses a field of values, ranges, steps and *, between lo and hi.
func parseSchedCronField(field string, lo, hi int, names map[string]int) (map[int]bool, error) {
	out := map[int]bool{}
	for _, part := range strings.Split(field, ",") {
		step := 1
		if base, s, ok := strings.Cut(part, "/"); ok {
			n, err := strconv.Atoi(s)
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("step %q", s)
			}
			step, part = n, base
		}
		from, to := lo, hi
		switch {
		case part == "*":
		case strings.Contains(part, "-"):
			a, b, _ := strings.Cut(part, "-")
			var err error
			if from, err = schedCronValue(a, names); err != nil {
				return nil, err
			}
			if to, err = schedCronValue(b, names); err != nil {
				return nil, err
			}
		default:
			v, err := schedCronValue(part, names)
			if err != nil {
				return nil, err
			}
			from = v
			if step == 1 {
				to = v
			}
		}
		if from < lo || to > hi || from > to {
			return nil, fmt.Errorf("%q is outside %d-%d", part, lo, hi)
		}
		for v := from; v <= to; v += step {
			out[v] = true
		}
	}
	return out, nil
}

// schedCronValue reads one number or, where the field has them, one name.
func schedCronValue(s string, names map[string]int) (int, error) {
	if v, ok := names[strings.ToUpper(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("value %q", s)
	}
	return v, nil
}

func parseSchedCronDom(field string) (schedCronDay, error) {
	switch {
	case field == "?":
		return schedCronDay{any: true}, nil
	case field == "L":
		return schedCronDay{last: true}, nil
	case strings.HasSuffix(field, "W"):
		n, err := strconv.Atoi(strings.TrimSuffix(field, "W"))
		if err != nil || n < 1 || n > 31 {
			return schedCronDay{}, fmt.Errorf("%q", field)
		}
		return schedCronDay{weekday: n}, nil
	}
	set, err := parseSchedCronField(field, 1, 31, nil)
	return schedCronDay{set: set}, err
}

func parseSchedCronDow(field string) (schedCronDay, error) {
	switch {
	case field == "?":
		return schedCronDay{any: true}, nil
	case field == "L":
		return schedCronDay{set: map[int]bool{7: true}}, nil
	case strings.HasSuffix(field, "L"):
		d, err := schedCronValue(strings.TrimSuffix(field, "L"), schedCronDayNames)
		if err != nil || d < 1 || d > 7 {
			return schedCronDay{}, fmt.Errorf("%q", field)
		}
		return schedCronDay{lastDow: d}, nil
	case strings.Contains(field, "#"):
		a, b, _ := strings.Cut(field, "#")
		d, err := schedCronValue(a, schedCronDayNames)
		if err != nil || d < 1 || d > 7 {
			return schedCronDay{}, fmt.Errorf("%q", field)
		}
		n, err := strconv.Atoi(b)
		if err != nil || n < 1 || n > 5 {
			return schedCronDay{}, fmt.Errorf("%q", field)
		}
		return schedCronDay{nthDow: [2]int{d, n}}, nil
	}
	set, err := parseSchedCronField(field, 1, 7, schedCronDayNames)
	return schedCronDay{set: set}, err
}

// dayMatches reports whether the calendar day y-m-d satisfies both day fields.
func (c *schedCron) dayMatches(y int, m time.Month, d int) bool {
	last := time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
	wd := int(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Weekday()) + 1 // 1 = SUN
	if !c.dom.any {
		switch {
		case c.dom.last:
			if d != last {
				return false
			}
		case c.dom.weekday > 0:
			if d != schedNearestWeekday(y, m, c.dom.weekday, last) {
				return false
			}
		default:
			if !c.dom.set[d] {
				return false
			}
		}
	}
	if !c.dow.any {
		switch {
		case c.dow.lastDow > 0:
			if wd != c.dow.lastDow || d+7 <= last {
				return false
			}
		case c.dow.nthDow[0] > 0:
			if wd != c.dow.nthDow[0] || (d-1)/7+1 != c.dow.nthDow[1] {
				return false
			}
		default:
			if !c.dow.set[wd] {
				return false
			}
		}
	}
	return true
}

// schedNearestWeekday returns the weekday (Monday to Friday) nearest day n of the month, never
// crossing into another month — the conventional reading of the published "nW".
func schedNearestWeekday(y int, m time.Month, n, last int) int {
	if n > last {
		n = last
	}
	switch time.Date(y, m, n, 0, 0, 0, 0, time.UTC).Weekday() {
	case time.Saturday:
		if n == 1 {
			return 3
		}
		return n - 1
	case time.Sunday:
		if n == last {
			return n - 2
		}
		return n + 1
	default:
		return n
	}
}

// atOrAfter returns the first occurrence at or after from, evaluated in loc, or nil when there is
// none before the published Year field's end.
func (c *schedCron) atOrAfter(from time.Time, loc *time.Location) *time.Time {
	local := from.In(loc)
	for y := local.Year(); y <= schedCronMaxYear; y++ {
		if !c.years[y] {
			continue
		}
		for m := time.January; m <= time.December; m++ {
			if !c.months[int(m)] || (y == local.Year() && m < local.Month()) {
				continue
			}
			last := time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
			for d := 1; d <= last; d++ {
				if y == local.Year() && m == local.Month() && d < local.Day() {
					continue
				}
				if !c.dayMatches(y, m, d) {
					continue
				}
				for h := 0; h < 24; h++ {
					if !c.hours[h] {
						continue
					}
					for mi := 0; mi < 60; mi++ {
						if !c.minutes[mi] {
							continue
						}
						t, ok := schedLocalInstant(y, m, d, h, mi, loc)
						if ok && !t.Before(from) {
							return &t
						}
					}
				}
			}
		}
	}
	return nil
}

// schedLocalInstant resolves a wall-clock time in loc to an instant, applying the guide's
// daylight-saving rules: a time the zone skips does not exist (false), and a time the zone repeats
// resolves to its second occurrence.
func schedLocalInstant(y int, m time.Month, d, h, mi int, loc *time.Location) (time.Time, bool) {
	t := time.Date(y, m, d, h, mi, 0, 0, loc)
	same := func(c time.Time) bool {
		l := c.In(loc)
		return l.Year() == y && l.Month() == m && l.Day() == d && l.Hour() == h && l.Minute() == mi
	}
	if !same(t) {
		return time.Time{}, false
	}
	best := t
	for _, off := range []time.Duration{30 * time.Minute, time.Hour, 2 * time.Hour} {
		if c := t.Add(off); same(c) && c.After(best) {
			best = c
		}
	}
	return best, true
}
