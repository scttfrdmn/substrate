package substrate_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate"
)

// newTestLogger creates a SlogLogger that writes to buf so tests can inspect output.
func newTestLogger(buf *bytes.Buffer, level slog.Level) substrate.Logger {
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})
	return substrate.NewSlogLogger(slog.New(handler))
}

func TestSlogLogger_Levels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		level slog.Level
		log   func(substrate.Logger)
		want  string
	}{
		{
			name:  "debug",
			level: slog.LevelDebug,
			log:   func(l substrate.Logger) { l.Debug("debug message", "k", "v") },
			want:  "debug message",
		},
		{
			name:  "info",
			level: slog.LevelInfo,
			log:   func(l substrate.Logger) { l.Info("info message", "k", "v") },
			want:  "info message",
		},
		{
			name:  "warn",
			level: slog.LevelInfo,
			log:   func(l substrate.Logger) { l.Warn("warn message", "k", "v") },
			want:  "warn message",
		},
		{
			name:  "error",
			level: slog.LevelInfo,
			log:   func(l substrate.Logger) { l.Error("error message", "k", "v") },
			want:  "error message",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			l := newTestLogger(&buf, tc.level)
			tc.log(l)
			if !strings.Contains(buf.String(), tc.want) {
				t.Fatalf("expected output to contain %q, got: %s", tc.want, buf.String())
			}
		})
	}
}

func TestSlogLogger_DebugFilteredAtInfoLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelInfo)
	l.Debug("should not appear")

	if buf.Len() != 0 {
		t.Fatalf("Debug should be suppressed at Info level, got: %s", buf.String())
	}
}

func TestSlogLogger_KeyValuePairs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	l.Info("test", "service", "s3", "operation", "PutObject")

	out := buf.String()
	if !strings.Contains(out, "service=s3") {
		t.Errorf("expected service=s3 in output, got: %s", out)
	}
	if !strings.Contains(out, "operation=PutObject") {
		t.Errorf("expected operation=PutObject in output, got: %s", out)
	}
}

func TestNewDefaultLogger_TextFormat(t *testing.T) {
	t.Parallel()
	// Smoke test: constructing a default logger must not panic.
	l := substrate.NewDefaultLogger(slog.LevelInfo, false)
	if l == nil {
		t.Fatal("NewDefaultLogger returned nil")
	}
}

func TestNewDefaultLogger_JSONFormat(t *testing.T) {
	t.Parallel()
	l := substrate.NewDefaultLogger(slog.LevelInfo, true)
	if l == nil {
		t.Fatal("NewDefaultLogger JSON returned nil")
	}
}

func TestSlogLogger_ImplementsInterface(t *testing.T) {
	t.Parallel()
	_ = substrate.NewSlogLogger(slog.Default())
}
