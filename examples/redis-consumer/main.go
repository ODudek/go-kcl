package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/ODudek/go-kcl/clientlibrary/awsclients"
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

	// 3. Build the Kinesis client from the SDK's default credential chain.
	// A *kinesis.Client you build yourself works as well.
	kinesisClient, err := awsclients.NewKinesisClient(context.Background(), kclConfig)
	if err != nil {
		log.Fatalf("Failed to create Kinesis client: %v", err)
	}

	// 4. Build the worker with the Redis checkpointer
	worker := wk.NewWorker(&processorFactory{}, kclConfig).
		WithKinesis(kinesisClient).
		WithCheckpointer(checkpointer)

	// 5. Start consuming
	if err := worker.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// 6. Graceful shutdown on SIGINT/SIGTERM
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

func (p *recordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) error {
	if len(input.Records) == 0 {
		return nil
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
	return nil
}

func (p *recordProcessor) Shutdown(input *kc.ShutdownInput) {
	fmt.Printf("[shutdown] Reason: %s\n",
		aws.ToString(kc.ShutdownReasonMessage(input.ShutdownReason)))

	if input.ShutdownReason == kc.TERMINATE {
		_ = input.Checkpointer.Checkpoint(nil)
	}
}
