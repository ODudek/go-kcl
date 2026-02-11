package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"

	redischk "github.com/ODudek/go-kcl/clientlibrary/checkpoint/redis"
	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	kc "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/logger"
)

// This example demonstrates running two independent KCL applications
// that share a single Redis instance without key collisions.
//
// Each application uses a different ApplicationName (and therefore TableName),
// which namespaces all Redis keys:
//   - App "orders":  kcl:orders:shard:shardId-000000000001
//   - App "events":  kcl:events:shard:shardId-000000000001
//
// Both can safely run against the same Redis.

func main() {
	log := logger.GetDefaultLogger()

	redisAddr := envOrDefault("REDIS_ADDRESS", "localhost:6379")
	redisPwd := os.Getenv("REDIS_PASSWORD")

	// App 1: "orders" — consumes the "orders-stream"
	ordersConfig := cfg.NewKinesisClientLibConfig(
		"orders", // ApplicationName → Redis namespace "orders"
		"orders-stream",
		"us-east-1",
		"orders-worker-1",
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(100).
		WithLogger(log)

	ordersCheckpointer := redischk.NewRedisCheckpoint(ordersConfig, redischk.RedisConfig{
		Address:  redisAddr,
		Password: redisPwd,
	})

	ordersWorker := wk.NewWorker(&logFactory{prefix: "orders"}, ordersConfig).
		WithCheckpointer(ordersCheckpointer)

	// App 2: "events" — consumes the "events-stream"
	eventsConfig := cfg.NewKinesisClientLibConfig(
		"events", // ApplicationName → Redis namespace "events"
		"events-stream",
		"us-east-1",
		"events-worker-1",
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(100).
		WithLogger(log)

	eventsCheckpointer := redischk.NewRedisCheckpoint(eventsConfig, redischk.RedisConfig{
		Address:  redisAddr,
		Password: redisPwd,
	})

	eventsWorker := wk.NewWorker(&logFactory{prefix: "events"}, eventsConfig).
		WithCheckpointer(eventsCheckpointer)

	// Start both workers
	var wg sync.WaitGroup
	for _, w := range []*wk.Worker{ordersWorker, eventsWorker} {
		wg.Add(1)
		go func(worker *wk.Worker) {
			defer wg.Done()
			if err := worker.Start(); err != nil {
				log.Errorf("Worker start failed: %v", err)
			}
		}(w)
	}

	// Graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Infof("Shutting down both workers...")
	ordersWorker.Shutdown()
	eventsWorker.Shutdown()
	wg.Wait()
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// --- Record Processor ---

type logFactory struct {
	prefix string
}

func (f *logFactory) CreateProcessor() kc.IRecordProcessor {
	return &logProcessor{prefix: f.prefix}
}

type logProcessor struct {
	prefix string
}

func (p *logProcessor) Initialize(input *kc.InitializationInput) {
	fmt.Printf("[%s][init] ShardId: %s\n", p.prefix, input.ShardId)
}

func (p *logProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	for _, r := range input.Records {
		fmt.Printf("[%s][record] Key=%s Data=%s\n",
			p.prefix,
			aws.ToString(r.PartitionKey),
			string(r.Data))
	}

	if len(input.Records) > 0 {
		lastSeq := input.Records[len(input.Records)-1].SequenceNumber
		_ = input.Checkpointer.Checkpoint(lastSeq)
	}
}

func (p *logProcessor) Shutdown(input *kc.ShutdownInput) {
	fmt.Printf("[%s][shutdown] Reason: %s\n",
		p.prefix,
		aws.ToString(kc.ShutdownReasonMessage(input.ShutdownReason)))

	if input.ShutdownReason == kc.TERMINATE {
		_ = input.Checkpointer.Checkpoint(nil)
	}
}
