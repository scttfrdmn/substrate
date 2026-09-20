package emulator

// EventBridge Scheduler applies every published ListSchedules filter before the page is cut (#1229).
//
// listSchedules read its three filters in three different places relative to the cut: `ScheduleGroup`
// chose which name index to load, `NamePrefix` filtered that index, and **`State` was applied inside
// the render loop**, on the records the page had already selected. So a state-filtered request was
// answered `MaxResults` schedules minus however many of *that page* failed the filter, while the
// `NextToken` alongside had been computed from the unfiltered listing.
//
// # Why that is a divergence rather than a shortcut
//
// `API_ListSchedules` publishes `State` in the same URI-parameter list as the other two and in the
// same words — *"If specified, only lists the schedules whose current state matches the given
// filter."* against `NamePrefix`'s *"Schedule name prefix to return the filtered list of
// resources."* — so it is a filter on the listing, and a listing is what the page is cut from. The
// response member is the ordinary cursor: *"Indicates whether there are additional results to
// retrieve. If the value is null, there are no more results."* Nothing on the page says a page may be
// shorter than the `MaxResults` it was asked for.
//
// Two observations changed, the second worse than the first:
//
//   - `?State=ENABLED&MaxResults=20` over a group of 40 where half are `DISABLED` answered fewer than
//     20 enabled schedules with a `NextToken`, where AWS answers 20.
//   - When every schedule on a page failed the filter, it answered `{"Schedules":[],"NextToken":"…"}`
//     with matches still to come. A caller that stops at an empty list — which several SDK paginator
//     idioms do in practice — concluded the filter matched nothing. AWS can produce that shape too,
//     but from server-side scan limits, never from applying a filter it publishes.
//
// # What this costs, and the reading that was not taken
//
// `State` lives in the record, not in the name index, so filtering ahead of the cut means loading
// every record in the group on every call rather than one page's worth. That is the deliberate choice:
// the alternative is to copy `State` into the name index so the filter stays index-only, which would
// duplicate a field of the record into an index and leave `UpdateSchedule` with two places to write
// it — the class #756 exists for. For an in-process emulator a `state.Get` per schedule in a group is
// not a cost worth that.
//
// # What is unchanged
//
// The offset now counts *matching* schedules, so a token issued with a `State` filter and replayed
// without one indexes into a different listing. That was already true of `NamePrefix`, and is recorded
// in scheduler_pagination.go as a property of every offset paginator in the tree: the token carries an
// offset and nothing else. This change does not widen it and does not narrow it.
//
// A name the index holds with no record behind it, and a record that will not decode, are still
// skipped. They are store inconsistencies rather than published filters — there is no request the
// caller could send to produce either, and the page publishes no code for "substrate's own index and
// records disagree" — so a refusal would be inventing one. What changed is that skipping one can no
// longer shorten a *page*: it shortens the listing being cut from, exactly as a filter does, and a
// page is short only when the listing has run out.
//
// Also unchanged, and still recorded in scheduler_pagination.go as not fixed here: a `MaxResults`
// above the published maximum of 100 is clamped rather than refused, and a value of zero or below is
// silently ignored in favor of substrate's own default of 20.
