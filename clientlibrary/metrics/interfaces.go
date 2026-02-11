/*
 * Copyright (c) 2018 VMware, Inc.
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

// Package metrics provides the MonitoringService interface for recording
// KCL worker metrics. Implementations are available for CloudWatch and
// Prometheus backends.
//
// The implementation is derived from https://github.com/patrobinson/gokini
package metrics

// MonitoringService records KCL worker metrics including record processing
// rate, bytes processed, stream lag, and lease acquisition/renewal events.
// Metrics are tracked per shard. The service lifecycle is:
// Init -> Start -> [metric recording] -> Shutdown.
type MonitoringService interface {
	// Init initializes the service with application context (used as metric labels/dimensions).
	Init(appName, streamName, workerID string) error

	// Start begins metric collection and reporting.
	Start() error

	// IncrRecordsProcessed records the count of successfully processed records for the shard.
	IncrRecordsProcessed(shard string, count int)

	// IncrBytesProcessed records the number of data bytes processed for the shard.
	IncrBytesProcessed(shard string, count int64)

	// MillisBehindLatest records how many milliseconds the consumer lags behind the stream tip.
	MillisBehindLatest(shard string, milliSeconds float64)

	// DeleteMetricMillisBehindLatest clears the lag metric for the shard (e.g. on lease loss).
	DeleteMetricMillisBehindLatest(shard string)

	// LeaseGained records that the worker acquired the lease for the shard.
	LeaseGained(shard string)

	// LeaseLost records that the worker lost the lease for the shard.
	LeaseLost(shard string)

	// LeaseRenewed records a successful lease renewal for the shard.
	LeaseRenewed(shard string)

	// RecordGetRecordsTime records the elapsed time (ms) for fetching a batch from Kinesis.
	RecordGetRecordsTime(shard string, time float64)

	// RecordProcessRecordsTime records the elapsed time (ms) for processing a batch of records.
	RecordProcessRecordsTime(shard string, time float64)

	// Shutdown stops metric collection and flushes any pending metrics.
	Shutdown()
}

// NoopMonitoringService implements MonitoringService as a no-op,
// useful for testing or when metrics collection should be disabled.
type NoopMonitoringService struct{}

func (NoopMonitoringService) Init(_, _, _ string) error { return nil }
func (NoopMonitoringService) Start() error              { return nil }
func (NoopMonitoringService) Shutdown()                 {}

func (NoopMonitoringService) IncrRecordsProcessed(_ string, _ int)         {}
func (NoopMonitoringService) IncrBytesProcessed(_ string, _ int64)         {}
func (NoopMonitoringService) MillisBehindLatest(_ string, _ float64)       {}
func (NoopMonitoringService) DeleteMetricMillisBehindLatest(_ string)      {}
func (NoopMonitoringService) LeaseGained(_ string)                         {}
func (NoopMonitoringService) LeaseLost(_ string)                           {}
func (NoopMonitoringService) LeaseRenewed(_ string)                        {}
func (NoopMonitoringService) RecordGetRecordsTime(_ string, _ float64)     {}
func (NoopMonitoringService) RecordProcessRecordsTime(_ string, _ float64) {}
