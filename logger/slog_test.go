package logger

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newJSONSlog(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level}))
}

func TestSlogLogger_WritesThroughTheGivenLogger(t *testing.T) {
	var buf bytes.Buffer
	log := NewSlogLogger(newJSONSlog(&buf, slog.LevelInfo)).WithFields(Fields{"shard": "s1"})

	log.Debugf("hidden %d", 1)
	log.Infof("lease %s", "renewed")
	log.Warnf("careful")
	log.Errorf("failed: %v", "timeout")

	out := buf.String()
	assert.NotContains(t, out, "hidden")
	assert.Contains(t, out, `"level":"INFO","msg":"lease renewed","shard":"s1"`)
	assert.Contains(t, out, `"level":"WARN","msg":"careful"`)
	assert.Contains(t, out, `"level":"ERROR","msg":"failed: timeout"`)
}

func TestSlogLogger_PanicfLogsAndPanics(t *testing.T) {
	var buf bytes.Buffer
	log := NewSlogLogger(newJSONSlog(&buf, slog.LevelInfo))

	assert.PanicsWithValue(t, "boom 1", func() { log.Panicf("boom %d", 1) })
	assert.Contains(t, buf.String(), `"msg":"boom 1"`)
}

// The default logger is created early, with the KCL configuration; it must
// follow a slog default the application sets afterwards.
func TestGetDefaultLogger_FollowsTheCurrentSlogDefault(t *testing.T) {
	log := GetDefaultLogger()

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(newJSONSlog(&buf, slog.LevelDebug))
	defer slog.SetDefault(previous)

	log.Debugf("visible %d", 2)

	assert.Contains(t, buf.String(), `"msg":"visible 2"`)
}
