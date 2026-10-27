package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"time"
)

// slogLogger adapts a *slog.Logger to Logger. A nil logger means
// slog.Default() at the time of each call; fields added by WithFields are
// then applied to that default too.
type slogLogger struct {
	logger *slog.Logger
	fields []interface{}
}

// NewSlogLogger adapts an existing slog logger to the Logger interface.
// The caller is responsible for configuring its handler and level.
func NewSlogLogger(logger *slog.Logger) Logger {
	return &slogLogger{logger: logger}
}

func (l *slogLogger) get() *slog.Logger {
	if l.logger != nil {
		return l.logger
	}
	if len(l.fields) == 0 {
		return slog.Default()
	}
	return slog.Default().With(l.fields...)
}

// log writes a record attributed to the caller of the Logger method, and
// returns its message. Messages at error level are always formatted, for
// Panicf.
func (l *slogLogger) log(level slog.Level, format string, args []interface{}) string {
	logger := l.get()
	ctx := context.Background()
	enabled := logger.Enabled(ctx, level)
	if !enabled && level < slog.LevelError {
		return ""
	}
	msg := fmt.Sprintf(format, args...)
	if enabled {
		var pcs [1]uintptr
		runtime.Callers(3, pcs[:]) // skip Callers, log and the Logger method
		_ = logger.Handler().Handle(ctx, slog.NewRecord(time.Now(), level, msg, pcs[0]))
	}
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
	if l.logger == nil {
		return &slogLogger{fields: append(append([]interface{}{}, l.fields...), args...)}
	}
	return &slogLogger{logger: l.logger.With(args...)}
}
