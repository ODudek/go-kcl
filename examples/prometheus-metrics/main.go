package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	kc "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	prommetrics "github.com/ODudek/go-kcl/clientlibrary/metrics/prometheus"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/logger"
)

func main() {
	log := logger.GetDefaultLogger()

	// 1. Configure KCL
	kclConfig := cfg.NewKinesisClientLibConfig(
		"my-app",
		"my-stream",
		"us-east-1",
		"worker-1",
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(100).
		WithLogger(log)

	// -----------------------------------------------------------------
	// Option A: Standalone mode (backward-compatible)
	//
	// KCL starts its own HTTP server on :2112 and registers metrics on
	// the global Prometheus registry.
	// -----------------------------------------------------------------
	_ = prommetrics.NewMonitoringService(":2112", "us-east-1", log)

	// -----------------------------------------------------------------
	// Option B: External registry
	//
	// When your application already exposes a Prometheus /metrics
	// endpoint, pass your own registry so KCL does not start a second
	// HTTP server.
	// -----------------------------------------------------------------
	registry := prom.NewRegistry()
	registry.MustRegister(prom.NewGoCollector())

	metricsService := prommetrics.NewMonitoringServiceWithOptions(
		prommetrics.WithRegistry(registry),
		prommetrics.WithRegion("us-east-1"),
		prommetrics.WithLogger(log),
	)

	// Attach the monitoring service to the worker config
	kclConfig.WithMonitoringService(metricsService)

	// Build and start the worker
	worker := wk.NewWorker(&processorFactory{}, kclConfig)
	if err := worker.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// Expose the external registry through the application's own HTTP server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	go func() {
		log.Infof("Serving Prometheus metrics on :2112")
		if err := http.ListenAndServe(":2112", mux); err != nil {
			log.Errorf("Metrics server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Infof("Received %s, shutting down...", sig)
	worker.Shutdown()
}

// --- Record Processor ---

type processorFactory struct{}

func (f *processorFactory) CreateProcessor() kc.IRecordProcessor {
	return &recordProcessor{}
}

type recordProcessor struct{}

func (p *recordProcessor) Initialize(input *kc.InitializationInput) {
	fmt.Printf("[init] ShardId: %s\n", input.ShardId)
}

func (p *recordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	if len(input.Records) == 0 {
		return
	}

	for _, r := range input.Records {
		fmt.Printf("[record] PartitionKey=%s Data=%s\n",
			aws.ToString(r.PartitionKey), string(r.Data))
	}

	lastSeq := input.Records[len(input.Records)-1].SequenceNumber
	if err := input.Checkpointer.Checkpoint(lastSeq); err != nil {
		fmt.Printf("[error] checkpoint failed: %v\n", err)
	}
}

func (p *recordProcessor) Shutdown(input *kc.ShutdownInput) {
	fmt.Printf("[shutdown] Reason: %s\n",
		aws.ToString(kc.ShutdownReasonMessage(input.ShutdownReason)))

	if input.ShutdownReason == kc.TERMINATE {
		_ = input.Checkpointer.Checkpoint(nil)
	}
}
