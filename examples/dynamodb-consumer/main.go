package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/ODudek/go-kcl/clientlibrary/awsclients"
	dynamochk "github.com/ODudek/go-kcl/clientlibrary/checkpoint/dynamodb"
	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	kc "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/logger"
)

func main() {
	log := logger.GetDefaultLogger()

	// 1. Configure KCL
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

	// 2. Build the AWS clients from the SDK's default credential chain.
	// Clients you build yourself work as well.
	ctx := context.Background()
	kinesisClient, err := awsclients.NewKinesisClient(ctx, kclConfig)
	if err != nil {
		log.Fatalf("Failed to create Kinesis client: %v", err)
	}
	dynamoClient, err := awsclients.NewDynamoDBClient(ctx, kclConfig)
	if err != nil {
		log.Fatalf("Failed to create DynamoDB client: %v", err)
	}

	// 3. Build the worker with a DynamoDB checkpointer. It creates the lease
	// table if missing (table name defaults to ApplicationName).
	worker := wk.NewWorker(&processorFactory{}, kclConfig).
		WithKinesis(kinesisClient).
		WithCheckpointer(dynamochk.NewDynamoCheckpoint(kclConfig).WithDynamoDB(dynamoClient))

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

// To use custom endpoints (e.g. localstack), set them before building the clients:
//
//   kclConfig.WithKinesisEndpoint("http://localhost:4566").
//       WithDynamoDBEndpoint("http://localhost:4566")
