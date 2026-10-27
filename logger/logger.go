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

// Package logger defines the Logger interface the KCL logs through, and a
// default implementation on the standard library's log/slog.
//
// Adapters for third-party loggers live in their own packages (logger/logrus,
// logger/zap, logger/zerolog), so an application links only the logger it
// uses. They wrap a logger the application has already configured; writing
// to and rotating log files is left to the application.
package logger

// Fields Type to pass when we want to call WithFields for structured logging
type Fields map[string]interface{}

// Logger is the common interface for logging.
type Logger interface {
	Debugf(format string, args ...interface{})

	Infof(format string, args ...interface{})

	Warnf(format string, args ...interface{})

	Errorf(format string, args ...interface{})

	Fatalf(format string, args ...interface{})

	Panicf(format string, args ...interface{})

	WithFields(keyValues Fields) Logger
}

// GetDefaultLogger returns a Logger that writes to slog.Default(), looked up
// at every call, so a default the application sets later is used too.
func GetDefaultLogger() Logger {
	return &slogLogger{}
}
