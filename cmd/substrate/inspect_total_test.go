package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// `substrate inspect <service>` asks the request log for the last 100 events and used to
// print the length of that page as if it were the run — #1237.
//
// The two cases here are the two answers the endpoint can give: a run that fits under the
// limit, which prints the count alone, and one that does not, which says so. Without the
// second the command reported `(100)` for a run that had recorded thousands, and a reader
// had no way to tell a hundred-request run from a truncated one.

// inspectHeader runs `substrate inspect <service>` against a stub request log returning
// body, and returns the first line the command printed.
func inspectHeader(t *testing.T, body string) string {
	t.Helper()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/debug/events") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	defer ts.Close()

	// The command prints with fmt.Printf, so stdout is the only place to read it from.
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	cmd := newInspectCmd()
	cmd.SetArgs([]string{"s3", "--address", ts.URL})
	runErr := cmd.Execute()

	if closeErr := w.Close(); closeErr != nil {
		t.Fatalf("close pipe: %v", closeErr)
	}
	os.Stdout = orig

	out, readErr := io.ReadAll(r)
	if readErr != nil {
		t.Fatalf("read pipe: %v", readErr)
	}
	if runErr != nil {
		t.Fatalf("inspect returned %v (output %q)", runErr, out)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return lines[0]
}

// TestInspectReportsTheWholeRunWhenThePageIsShort is the false negative #1237 fixed in the
// CLI: the page is the newest 100 events, so its length is not the run's length.
func TestInspectReportsTheWholeRunWhenThePageIsShort(t *testing.T) {
	const event = `{"seq":1,"timestamp":"2026-01-01T00:00:00Z","operation":"PutObject","status_code":200}`
	header := inspectHeader(t, fmt.Sprintf(`{"events":[%s],"count":100,"total":1234,"truncated":true}`, event))

	want := "Recent events for s3 (showing 100 of 1234):"
	if header != want {
		t.Errorf("header = %q, want %q", header, want)
	}
}

// TestInspectReportsACountAloneWhenNothingWasTrimmed pins the unchanged line, so the
// truncation notice is a difference rather than a constant both cases would satisfy.
func TestInspectReportsACountAloneWhenNothingWasTrimmed(t *testing.T) {
	header := inspectHeader(t, `{"events":[],"count":3,"total":3,"truncated":false}`)

	want := "Recent events for s3 (3):"
	if header != want {
		t.Errorf("header = %q, want %q", header, want)
	}
}
