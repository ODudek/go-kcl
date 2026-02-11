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

// Package prometheus
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package prometheus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ODudek/go-kcl/logger"
)

// MonitoringService publishes KCL metrics to Prometheus.
//
// Two modes of operation:
//   - Standalone (default / backward-compatible): registers metrics on the
//     global registry and starts its own HTTP server.
//   - External registry: the caller provides a *prom.Registry (or
//     prom.Registerer); no HTTP server is started and the caller is
//     responsible for exposing metrics.
type MonitoringService struct {
	listenAddress string
	namespace     string
	streamName    string
	workerID      string
	region        string
	logger        logger.Logger

	registerer  prom.Registerer
	gatherer    prom.Gatherer
	startServer bool
	server      *http.Server

	processedRecords   *prom.CounterVec
	processedBytes     *prom.CounterVec
	behindLatestMillis *prom.GaugeVec
	leasesHeld         *prom.GaugeVec
	leaseRenewals      *prom.CounterVec
	getRecordsTime     *prom.HistogramVec
	processRecordsTime *prom.HistogramVec
}

// NewMonitoringService returns a MonitoringService that registers metrics on
// the global Prometheus registry and starts its own HTTP server on
// listenAddress. This preserves the original constructor signature for
// backward compatibility.
func NewMonitoringService(listenAddress, region string, log logger.Logger) *MonitoringService {
	return NewMonitoringServiceWithOptions(
		WithListenAddress(listenAddress),
		WithRegion(region),
		WithLogger(log),
	)
}

// NewMonitoringServiceWithOptions creates a MonitoringService configured via
// functional options. When no WithRegistry / WithRegisterer option is
// supplied the service behaves identically to NewMonitoringService (global
// registry, own HTTP server).
func NewMonitoringServiceWithOptions(opts ...Option) *MonitoringService {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	return &MonitoringService{
		listenAddress: cfg.listenAddress,
		region:        cfg.region,
		logger:        cfg.logger,
		registerer:    cfg.registerer,
		gatherer:      cfg.gatherer,
		startServer:   cfg.startServer,
	}
}

func (p *MonitoringService) Init(appName, streamName, workerID string) error {
	p.namespace = appName
	p.streamName = streamName
	p.workerID = workerID

	p.processedBytes = prom.NewCounterVec(prom.CounterOpts{
		Name: p.namespace + `_processed_bytes`,
		Help: "Number of bytes processed",
	}, []string{"kinesisStream", "shard"})
	p.processedRecords = prom.NewCounterVec(prom.CounterOpts{
		Name: p.namespace + `_processed_records`,
		Help: "Number of records processed",
	}, []string{"kinesisStream", "shard"})
	p.behindLatestMillis = prom.NewGaugeVec(prom.GaugeOpts{
		Name: p.namespace + `_behind_latest_millis`,
		Help: "The amount of milliseconds processing is behind",
	}, []string{"kinesisStream", "shard"})
	p.leasesHeld = prom.NewGaugeVec(prom.GaugeOpts{
		Name: p.namespace + `_leases_held`,
		Help: "The number of leases held by the worker",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.leaseRenewals = prom.NewCounterVec(prom.CounterOpts{
		Name: p.namespace + `_lease_renewals`,
		Help: "The number of successful lease renewals",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.getRecordsTime = prom.NewHistogramVec(prom.HistogramOpts{
		Name: p.namespace + `_get_records_duration_milliseconds`,
		Help: "The time taken to fetch records and process them",
	}, []string{"kinesisStream", "shard"})
	p.processRecordsTime = prom.NewHistogramVec(prom.HistogramOpts{
		Name: p.namespace + `_process_records_duration_milliseconds`,
		Help: "The time taken to process records",
	}, []string{"kinesisStream", "shard"})

	collectors := []prom.Collector{
		p.processedBytes,
		p.processedRecords,
		p.behindLatestMillis,
		p.leasesHeld,
		p.leaseRenewals,
		p.getRecordsTime,
		p.processRecordsTime,
	}
	for _, c := range collectors {
		if err := p.registerer.Register(c); err != nil {
			var are prom.AlreadyRegisteredError
			if errors.As(err, &are) {
				continue
			}
			return fmt.Errorf("registering collector: %w", err)
		}
	}

	return nil
}

func (p *MonitoringService) Start() error {
	if !p.startServer {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(p.gatherer, promhttp.HandlerOpts{}))

	p.server = &http.Server{
		Addr:              p.listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		p.logger.Infof("Starting Prometheus listener on %s", p.listenAddress)
		if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Errorf("Error starting Prometheus metrics endpoint. %+v", err)
		}
		p.logger.Infof("Stopped metrics server")
	}()

	return nil
}

func (p *MonitoringService) Shutdown() {
	if p.server == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.server.Shutdown(ctx); err != nil {
		p.logger.Errorf("Error shutting down Prometheus metrics server: %+v", err)
	}
}

func (p *MonitoringService) IncrRecordsProcessed(shard string, count int) {
	p.processedRecords.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Add(float64(count))
}

func (p *MonitoringService) IncrBytesProcessed(shard string, count int64) {
	p.processedBytes.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Add(float64(count))
}

func (p *MonitoringService) MillisBehindLatest(shard string, millSeconds float64) {
	p.behindLatestMillis.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Set(millSeconds)
}

func (p *MonitoringService) DeleteMetricMillisBehindLatest(shard string) {
	p.behindLatestMillis.Delete(prom.Labels{"shard": shard, "kinesisStream": p.streamName})
}

func (p *MonitoringService) LeaseGained(shard string) {
	p.leasesHeld.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName, "workerID": p.workerID}).Inc()
}

func (p *MonitoringService) LeaseLost(shard string) {
	p.leasesHeld.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName, "workerID": p.workerID}).Dec()
}

func (p *MonitoringService) LeaseRenewed(shard string) {
	p.leaseRenewals.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName, "workerID": p.workerID}).Inc()
}

func (p *MonitoringService) RecordGetRecordsTime(shard string, time float64) {
	p.getRecordsTime.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Observe(time)
}

func (p *MonitoringService) RecordProcessRecordsTime(shard string, time float64) {
	p.processRecordsTime.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Observe(time)
}
