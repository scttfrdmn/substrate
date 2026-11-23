package e2e_test

import "time"

// journeyEpoch is the instant the journeys that run under emulator.RunTwice pin the clock at
// (#1628).
//
// Each such journey runs twice, against two fresh servers in deterministic-run mode, and fails
// unless the two recorded the same events with byte-identical response bodies. That is the
// conformance check deterministic-run mode (#1598) promises and its own acceptance test covers
// for only a handful of operations: a body whose order comes from iterating a Go map, on a path
// nobody audited, shows up here as a difference between the runs.
//
// A journey qualifies when it starts its own server, issues its requests one at a time, and
// neither reads the wall clock nor moves the simulated one. The journeys that share TestMain's
// server, run requests concurrently, or exercise the clock itself (time travel, the vCPU quota
// window) are left as they are.
var journeyEpoch = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
