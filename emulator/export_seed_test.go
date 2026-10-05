package emulator

import "testing"

// RedirectSeedsForTest points ts's seed helpers at state and has them report to tb, so an
// external test can arm a failing store under them and see the failure reported (#1192).
//
// Exported because neither is reachable otherwise: StartTestServer builds its own in-memory
// store, which never fails a write, and binds the seed helpers to the test that started it,
// which a test cannot observe failing without failing itself.
func (ts *TestServer) RedirectSeedsForTest(tb testing.TB, state StateManager) {
	ts.tb = tb
	ts.state = state
}
