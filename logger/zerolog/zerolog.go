/*
 * Copyright (c) 2019 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// Note: The implementation comes from https://www.mountedthoughts.com/golang-logger-interface/
// https://github.com/amitrai48/logger

// Package zerolog implements the KCL logger using RS Zerolog logger
package zerolog

import (
	"github.com/ODudek/go-kcl/logger"
	"github.com/rs/zerolog"
)

type zeroLogger struct {
	log zerolog.Logger
}

// NewZerologLogger adapts an existing zerolog logger to the Logger interface.
// The caller is responsible for configuring its output and level.
func NewZerologLogger(log zerolog.Logger) logger.Logger {
	return &zeroLogger{log: log}
}

func (z *zeroLogger) Debugf(format string, args ...interface{}) {
	z.log.Debug().Msgf(format, args...)
}

func (z *zeroLogger) Infof(format string, args ...interface{}) {
	z.log.Info().Msgf(format, args...)
}

func (z *zeroLogger) Warnf(format string, args ...interface{}) {
	z.log.Warn().Msgf(format, args...)
}

func (z *zeroLogger) Errorf(format string, args ...interface{}) {
	z.log.Error().Msgf(format, args...)
}

func (z *zeroLogger) Fatalf(format string, args ...interface{}) {
	z.log.Fatal().Msgf(format, args...)
}

func (z *zeroLogger) Panicf(format string, args ...interface{}) {
	z.log.Panic().Msgf(format, args...)
}

func (z *zeroLogger) WithFields(keyValues logger.Fields) logger.Logger {
	newLogger := z.log.With()
	for k, v := range keyValues {
		newLogger = newLogger.Interface(k, v)
	}

	return &zeroLogger{
		log: newLogger.Logger(),
	}
}
