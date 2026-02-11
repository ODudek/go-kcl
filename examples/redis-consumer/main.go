package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"

	redischk "github.com/ODudek/go-kcl/clientlibrary/checkpoint/redis"
	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	kc "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/logger"
)

func main() {
	log := logger.GetDefaultLogger()

	// 1. Configure KCL
	kclConfig := cfg.NewKinesisClientLibConfig(
		"my-app",    // applicationName (also used as Redis key namespace)
		"my-stream", // streamName
		"us-east-1", // regionName
		"worker-1",  // workerID
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(100).
		WithFailoverTimeMillis(10000).
		WithLogger(log)

	// 2. Create a Redis-backed checkpointer
	checkpointer := redischk.NewRedisCheckpoint(kclConfig, redischk.RedisConfig{
		Address:  envOrDefault("REDIS_ADDRESS", "localhost:6379"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})

	// 3. Build the worker with the Redis checkpointer
	worker := wk.NewWorker(&processorFactory{}, kclConfig).
		WithCheckpointer(checkpointer)

	// 4. Start consuming
	if err := worker.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// 5. Graceful shutdown on SIGINT/SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Infof("Received %s, shutting down...", sig)
	worker.Shutdown()
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// --- Record Processor ---

type processorFactory struct{}

func (f *processorFactory) CreateProcessor() kc.IRecordProcessor {
	return &recordProcessor{}
}

type recordProcessor struct{}

func (p *recordProcessor) Initialize(input *kc.InitializationInput) {
	fmt.Printf("[init] ShardId: %s, Checkpoint: %s\n",
		input.ShardId,
		aws.ToString(input.ExtendedSequenceNumber.SequenceNumber))
}

func (p *recordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	if len(input.Records) == 0 {
		return
	}

	for _, r := range input.Records {
		fmt.Printf("[record] PartitionKey=%s Data=%s\n",
			aws.ToString(r.PartitionKey), string(r.Data))
	}

	// Checkpoint after processing the batch
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
