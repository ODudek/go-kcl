package zap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	uzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/ODudek/go-kcl/logger"
	"github.com/ODudek/go-kcl/logger/zap"
)

func TestZapLogger_WritesThroughTheGivenLogger(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	log := zap.NewZapLogger(uzap.New(core).Sugar()).WithFields(logger.Fields{"shard": "s1"})

	log.Debugf("hidden %d", 1)
	log.Infof("lease %s", "renewed")

	require.Equal(t, 1, logs.Len())
	entry := logs.All()[0]
	assert.Equal(t, "lease renewed", entry.Message)
	assert.Equal(t, "s1", entry.ContextMap()["shard"])
}

func TestZapLogger_PanicfPanics(t *testing.T) {
	core, _ := observer.New(zapcore.InfoLevel)
	log := zap.NewZapLogger(uzap.New(core).Sugar())

	assert.Panics(t, func() { log.Panicf("boom") })
}
