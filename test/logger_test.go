package test

import (
	"log/slog"
	"os"

	"github.com/ODudek/go-kcl/logger"
)

// newTestLogger logs as text to stderr at the given level.
func newTestLogger(level slog.Level) logger.Logger {
	return logger.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
