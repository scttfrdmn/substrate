package emulator

// EventBridge Scheduler spells its query-string parameters two different ways, and the split is the
// API Reference's own rather than substrate's (#1226).
//
// Substrate read all five of ListSchedules' parameters in the lowerCamel form the single-schedule
// operations use, so a request from any SDK — which can only send the published form — arrived with
// none of them set. The observable result was the worst kind: ListSchedules answered the default
// group's first twenty schedules to every call, with a NextToken the next call then ignored, so a
// paginating loop either spun or reread the same page, and no filter had any effect. Nothing in a
// response said so.
//
// The keys are named here rather than written at the call sites so the two spellings sit next to each
// other and neither can be "corrected" into the other later.
//
// # Where each spelling comes from
//
// API_ListSchedules publishes *"GET /schedules?MaxResults={MaxResults}&NamePrefix={NamePrefix}&
// NextToken={NextToken}&ScheduleGroup={GroupName}&State={State}"* — PascalCase throughout, and note
// that the group is bound to ScheduleGroup even though the parameter list calls it GroupName, so
// ScheduleGroup is the key on the wire and GroupName never appears there.
//
// API_GetSchedule publishes *"GET /schedules/{Name}?groupName={GroupName}"* and API_DeleteSchedule
// publishes *"DELETE /schedules/{Name}?clientToken={ClientToken}&groupName={GroupName}"* —
// lowerCamel, and a different key name for the same concept. Those two handlers are therefore
// already right, and the fix deliberately does not sweep them.
//
// # What is not accepted
//
// The lowerCamel names are not kept as aliases for ListSchedules. AWS ignores a query parameter its
// model does not carry, so honoring a name the page does not publish would be the same class of
// defect in the other direction: a call that works against substrate and silently does nothing
// against AWS. A caller who sends ?maxResults=1 now reads the default page, which is what AWS
// answers.
const (
	// schedListGroupKey is ListSchedules' schedule-group filter, bound to GroupName on the page.
	schedListGroupKey = "ScheduleGroup"
	// schedListNamePrefixKey is ListSchedules' name-prefix filter.
	schedListNamePrefixKey = "NamePrefix"
	// schedListStateKey is ListSchedules' state filter.
	schedListStateKey = "State"
	// schedListNextTokenKey is ListSchedules' pagination cursor.
	schedListNextTokenKey = "NextToken"
	// schedListMaxResultsKey is ListSchedules' page-size request.
	schedListMaxResultsKey = "MaxResults"
	// schedGroupNameKey is the group parameter GetSchedule and DeleteSchedule publish, in the
	// lowerCamel form those two pages use. It is not ListSchedules' key.
	schedGroupNameKey = "groupName"
)

// schedListDefaultMaxResults is the page size ListSchedules applies when MaxResults is absent.
//
// This figure is substrate's, not the page's: API_ListSchedules publishes a Valid Range of 1–100 for
// MaxResults and no default at all, so there is nothing to match. Named so the reading is stated once
// rather than implied by a literal.
const schedListDefaultMaxResults = 20

// schedListMaxMaxResults is the published maximum of ListSchedules' MaxResults Valid Range.
//
// Substrate clamps a larger request to it rather than refusing, which is a divergence from the
// published range and is recorded in docs/services.md rather than fixed here — refusing it is the
// page-size class, not this one.
const schedListMaxMaxResults = 100
