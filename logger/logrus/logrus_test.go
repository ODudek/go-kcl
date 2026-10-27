package logrus_test

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/ODudek/go-kcl/logger"
	kcllogrus "github.com/ODudek/go-kcl/logger/logrus"
)

func newLogrus(buf *bytes.Buffer) *logrus.Logger {
	l := logrus.New()
	l.SetOutput(buf)
	l.SetFormatter(&logrus.JSONFormatter{})
	l.SetLevel(logrus.InfoLevel)
	return l
}

func TestLogrusLogger_WritesThroughTheGivenLogger(t *testing.T) {
	var buf bytes.Buffer
	log := kcllogrus.NewLogrusLogger(newLogrus(&buf)).WithFields(logger.Fields{"shard": "s1"})

	log.Debugf("hidden %d", 1)
	log.Infof("lease %s", "renewed")

	assert.NotContains(t, buf.String(), "hidden")
	assert.Contains(t, buf.String(), `"msg":"lease renewed"`)
	assert.Contains(t, buf.String(), `"shard":"s1"`)
}

func TestLogrusLogger_AcceptsAnEntry(t *testing.T) {
	var buf bytes.Buffer
	log := kcllogrus.NewLogrusLogger(newLogrus(&buf).WithField("app", "kcl"))

	log.Warnf("careful")

	assert.Contains(t, buf.String(), `"app":"kcl"`)
	assert.Contains(t, buf.String(), `"level":"warning"`)
}

func TestLogrusLogger_PanicfPanics(t *testing.T) {
	var buf bytes.Buffer
	log := kcllogrus.NewLogrusLogger(newLogrus(&buf))

	assert.Panics(t, func() { log.Panicf("boom") })
}
