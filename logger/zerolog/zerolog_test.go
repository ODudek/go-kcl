package zerolog

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/ODudek/go-kcl/logger"
)

func TestZerologLogger_WritesThroughTheGivenLogger(t *testing.T) {
	var buf bytes.Buffer
	log := NewZerologLogger(zerolog.New(&buf).Level(zerolog.InfoLevel)).
		WithFields(logger.Fields{"shard": "s1", "worker": "w1"})

	log.Debugf("hidden %d", 1)
	log.Infof("lease %s", "renewed")

	assert.NotContains(t, buf.String(), "hidden")
	assert.Contains(t, buf.String(), `"message":"lease renewed"`)
	assert.Contains(t, buf.String(), `"shard":"s1"`)
	assert.Contains(t, buf.String(), `"worker":"w1"`)
}

func TestZerologLogger_PanicfPanics(t *testing.T) {
	var buf bytes.Buffer
	log := NewZerologLogger(zerolog.New(&buf))

	assert.Panics(t, func() { log.Panicf("boom") })
}
