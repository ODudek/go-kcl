package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"

	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	kc "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/logger"
)

func main() {
	log := logger.GetDefaultLogger()

	// 1. Configure KCL
	//
	// DynamoDB is the default checkpointer — the worker creates the lease table
	// automatically (table name defaults to ApplicationName).
	kclConfig := cfg.NewKinesisClientLibConfig(
		"my-app",    // applicationName (also used as DynamoDB table name)
		"my-stream", // streamName
		"us-east-1", // regionName
		"worker-1",  // workerID
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(100).
		WithFailoverTimeMillis(10000).
		WithLogger(log)

	// 2. Build the worker — DynamoDB checkpointer is used by default
	worker := wk.NewWorker(&processorFactory{}, kclConfig)

	// 3. Start consuming
	if err := worker.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// 4. Graceful shutdown on SIGINT/SIGTERM
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

// To use a custom DynamoDB endpoint (e.g. localstack):
//
//   kclConfig.WithDynamoDBEndpoint("http://localhost:4566")
//
// To inject a custom DynamoDB client:
//
//   import chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
//
//   checkpointer := chk.NewDynamoCheckpoint(kclConfig).WithDynamoDB(dynamoClient)
//   worker := wk.NewWorker(factory, kclConfig).WithCheckpointer(checkpointer)
