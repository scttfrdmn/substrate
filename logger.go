package substrate

import (
	"log/slog"
	"os"
)

// SlogLogger wraps a [*slog.Logger] to satisfy the [Logger] interface.
type SlogLogger struct {
	l *slog.Logger
}

// NewSlogLogger creates a [Logger] backed by the provided slog.Logger.
func NewSlogLogger(l *slog.Logger) Logger {
	return &SlogLogger{l: l}
}

// NewDefaultLogger creates a [Logger] backed by a freshly constructed
// slog.Logger. When json is true the output uses JSON format; otherwise
// text format is used. Both write to os.Stderr.
func NewDefaultLogger(level slog.Level, json bool) Logger {
	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if json {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return &SlogLogger{l: slog.New(handler)}
}

// Debug logs msg at debug level with optional key-value args.
func (s *SlogLogger) Debug(msg string, args ...any) {
	s.l.Debug(msg, args...)
}

// Info logs msg at info level with optional key-value args.
func (s *SlogLogger) Info(msg string, args ...any) {
	s.l.Info(msg, args...)
}

// Warn logs msg at warning level with optional key-value args.
func (s *SlogLogger) Warn(msg string, args ...any) {
	s.l.Warn(msg, args...)
}

// Error logs msg at error level with optional key-value args.
func (s *SlogLogger) Error(msg string, args ...any) {
	s.l.Error(msg, args...)
}
