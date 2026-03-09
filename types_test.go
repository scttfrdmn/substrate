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
