package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
)

// slogLogger adapts a *slog.Logger to Logger. A nil logger means
// slog.Default() at the time of each call.
type slogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger adapts an existing slog logger to the Logger interface.
// The caller is responsible for configuring its handler and level.
func NewSlogLogger(logger *slog.Logger) Logger {
	return &slogLogger{logger: logger}
}

func (l *slogLogger) get() *slog.Logger {
	if l.logger == nil {
		return slog.Default()
	}
	return l.logger
}

func (l *slogLogger) log(level slog.Level, format string, args []interface{}) string {
	logger := l.get()
	ctx := context.Background()
	if !logger.Enabled(ctx, level) && level < slog.LevelError {
		return ""
	}
	msg := fmt.Sprintf(format, args...)
	logger.Log(ctx, level, msg)
	return msg
}

func (l *slogLogger) Debugf(format string, args ...interface{}) {
	l.log(slog.LevelDebug, format, args)
}

func (l *slogLogger) Infof(format string, args ...interface{}) {
	l.log(slog.LevelInfo, format, args)
}

func (l *slogLogger) Warnf(format string, args ...interface{}) {
	l.log(slog.LevelWarn, format, args)
}

func (l *slogLogger) Errorf(format string, args ...interface{}) {
	l.log(slog.LevelError, format, args)
}

// Fatalf logs at error level, slog having no fatal level, and exits.
func (l *slogLogger) Fatalf(format string, args ...interface{}) {
	l.log(slog.LevelError, format, args)
	os.Exit(1)
}

// Panicf logs at error level and panics with the message.
func (l *slogLogger) Panicf(format string, args ...interface{}) {
	panic(l.log(slog.LevelError, format, args))
}

// WithFields returns a Logger that adds fields, sorted by key, to every record.
func (l *slogLogger) WithFields(fields Fields) Logger {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := make([]interface{}, 0, 2*len(keys))
	for _, k := range keys {
		args = append(args, k, fields[k])
	}
	return &slogLogger{logger: l.get().With(args...)}
}
