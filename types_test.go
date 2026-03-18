package substrate_test

import (
	"sync"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate"
)

// TestTimeController_ConcurrentAccess verifies that concurrent reads and writes
// to a TimeController do not produce data races. The test passes iff
// go test -race reports no race conditions.
func TestTimeController_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	tc := substrate.NewTimeController(time.Now())

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// 50 goroutines calling SetTime concurrently.
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			tc.SetTime(time.Now().Add(time.Duration(i) * time.Second))
		}(i)
	}

	// 50 goroutines calling Now concurrently.
	for range goroutines {
		go func() {
			defer wg.Done()
			_ = tc.Now()
		}()
	}

	wg.Wait()
}

// TestTimeController_SetScale_ConcurrentAccess verifies that concurrent calls
// to SetScale and Now do not produce data races.
func TestTimeController_SetScale_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	tc := substrate.NewTimeController(time.Now())

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			tc.SetScale(float64(i + 1))
		}(i)
	}

	for range goroutines {
		go func() {
			defer wg.Done()
			_ = tc.Now()
		}()
	}

	wg.Wait()
}

func TestTimeController_SetTime_MovesNow(t *testing.T) {
	t.Parallel()
	future := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(time.Now())
	tc.SetTime(future)
	// Now() must be at or after the target (wall time may add a tiny delta).
	got := tc.Now()
	if got.Before(future) {
		t.Errorf("Now() = %v; want >= %v after SetTime", got, future)
	}
}

func TestTimeController_ScaleAdvancesTime(t *testing.T) {
	t.Parallel()
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(start)
	// Scale of 1000: 10ms of wall time should advance ~10s of simulated time.
	tc.SetScale(1000)
	time.Sleep(10 * time.Millisecond)
	got := tc.Now()
	elapsed := got.Sub(start)
	if elapsed < 5*time.Second {
		t.Errorf("after 10ms at scale 1000, simulated elapsed = %v; want >= 5s", elapsed)
	}
}

func TestTimeController_SetScale_NoJump(t *testing.T) {
	// Changing the scale must not produce a backward jump in simulated time.
	t.Parallel()
	tc := substrate.NewTimeController(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))
	tc.SetScale(100)
	time.Sleep(5 * time.Millisecond)
	before := tc.Now()
	tc.SetScale(1)
	after := tc.Now()
	if after.Before(before) {
		t.Errorf("SetScale caused backward jump: before=%v after=%v", before, after)
	}
}
