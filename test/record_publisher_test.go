/*
 * Copyright (c) 2020 VMware, Inc.
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
package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/ODudek/go-kcl/clientlibrary/awsclients"
	dynamochk "github.com/ODudek/go-kcl/clientlibrary/checkpoint/dynamodb"
	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	kcl "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	"github.com/ODudek/go-kcl/clientlibrary/utils"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/internal/kpl"
)

const specstr = `{"name":"kube-qQyhk","networking":{"containerNetworkCidr":"10.2.0.0/16"},"orgName":"BVT-Org-cLQch","projectName":"project-tDSJd","serviceLevel":"DEVELOPER","size":{"count":1},"version":"1.8.1-4"}`

// NewKinesisClient to create a Kinesis Client.
func NewKinesisClient(t *testing.T, regionName, endpoint string, creds aws.CredentialsProvider) *kinesis.Client {
	t.Helper()
	kclConfig := cfg.NewKinesisClientLibConfigWithCredential(appName, streamName, regionName, workerID, creds).
		WithKinesisEndpoint(endpoint)
	kc, err := awsclients.NewKinesisClient(context.TODO(), kclConfig)
	if err != nil {
		t.Fatalf("Failed in creating Kinesis client: %+v", err)
	}
	return kc
}

// NewDynamoDBClient to create a DynamoDB Client.
func NewDynamoDBClient(t *testing.T, regionName, endpoint string, creds aws.CredentialsProvider) *dynamodb.Client {
	t.Helper()
	kclConfig := cfg.NewKinesisClientLibConfigWithCredential(appName, streamName, regionName, workerID, creds).
		WithDynamoDBEndpoint(endpoint)
	dc, err := awsclients.NewDynamoDBClient(context.TODO(), kclConfig)
	if err != nil {
		t.Fatalf("Failed in creating DynamoDB client: %+v", err)
	}
	return dc
}

// newDynamoCheckpoint returns a DynamoDB checkpointer for kclConfig.
func newDynamoCheckpoint(t *testing.T, kclConfig *cfg.KinesisClientLibConfiguration) *dynamochk.DynamoCheckpoint {
	t.Helper()
	dc := NewDynamoDBClient(t, kclConfig.RegionName, kclConfig.DynamoDBEndpoint, kclConfig.DynamoDBCredentials)
	return dynamochk.NewDynamoCheckpoint(kclConfig).WithDynamoDB(dc)
}

// newTestWorker returns a worker reading through a Kinesis client and
// checkpointing to DynamoDB, both built from kclConfig.
func newTestWorker(t *testing.T, factory kcl.IRecordProcessorFactory, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	t.Helper()
	kc := NewKinesisClient(t, kclConfig.RegionName, kclConfig.KinesisEndpoint, kclConfig.KinesisCredentials)
	return wk.NewWorker(factory, kclConfig).
		WithKinesis(kc).
		WithCheckpointer(newDynamoCheckpoint(t, kclConfig))
}

func continuouslyPublishSomeData(t *testing.T, kc *kinesis.Client) func() {
	var shards []types.Shard
	var nextToken *string
	for {
		out, err := kc.ListShards(context.TODO(), &kinesis.ListShardsInput{
			StreamName: aws.String(streamName),
			NextToken:  nextToken,
		})
		if err != nil {
			t.Errorf("Error in ListShards. %+v", err)
		}

		shards = append(shards, out.Shards...)
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	done := make(chan int)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				publishToAllShards(t, kc, shards)
				publishSomeData(t, kc)
			}
		}
	}()

	return func() {
		close(done)
		wg.Wait()
	}
}

func publishToAllShards(t *testing.T, kc *kinesis.Client, shards []types.Shard) {
	// Put records to all shards
	for i := 0; i < 10; i++ {
		for _, shard := range shards {
			publishRecord(t, kc, shard.HashKeyRange.StartingHashKey)
		}
	}
}

// publishSomeData to put some records into Kinesis stream
func publishSomeData(t *testing.T, kc *kinesis.Client) {
	// Put some data into stream.
	t.Log("Putting data into stream using PutRecord API...")
	for i := 0; i < 50; i++ {
		publishRecord(t, kc, nil)
	}
	t.Log("Done putting data into stream using PutRecord API.")

	// Put some data into stream using PutRecords API
	t.Log("Putting data into stream using PutRecords API...")
	for i := 0; i < 10; i++ {
		publishRecords(t, kc)
	}
	t.Log("Done putting data into stream using PutRecords API.")

	// Put some data into stream using KPL Aggregate Record format
	t.Log("Putting data into stream using KPL Aggregate Record ...")
	for i := 0; i < 10; i++ {
		publishAggregateRecord(t, kc)
	}
	t.Log("Done putting data into stream using KPL Aggregate Record.")
}

// publishRecord to put a record into Kinesis stream using PutRecord API.
func publishRecord(t *testing.T, kc *kinesis.Client, hashKey *string) {
	input := &kinesis.PutRecordInput{
		Data:         []byte(specstr),
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
	}
	if hashKey != nil {
		input.ExplicitHashKey = hashKey
	}
	// Use random string as partition key to ensure even distribution across shards
	_, err := kc.PutRecord(context.TODO(), input)

	if err != nil {
		t.Errorf("Error in PutRecord. %+v", err)
	}
}

// publishRecord to put a record into Kinesis stream using PutRecords API.
func publishRecords(t *testing.T, kc *kinesis.Client) {
	// Use random string as partition key to ensure even distribution across shards
	records := make([]types.PutRecordsRequestEntry, 5)

	for i := 0; i < 5; i++ {
		record := types.PutRecordsRequestEntry{
			Data:         []byte(specstr),
			PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
		}
		records[i] = record
	}

	_, err := kc.PutRecords(context.TODO(), &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(streamName),
	})

	if err != nil {
		t.Errorf("Error in PutRecords. %+v", err)
	}
}

// publishRecord to put a record into Kinesis stream using PutRecord API.
func publishAggregateRecord(t *testing.T, kc *kinesis.Client) {
	data := generateAggregateRecord(5, specstr)
	// Use random string as partition key to ensure even distribution across shards
	_, err := kc.PutRecord(context.TODO(), &kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
	})

	if err != nil {
		t.Errorf("Error in PutRecord. %+v", err)
	}
}

// generateAggregateRecord generates an aggregate record in the correct AWS-specified format used by KPL.
// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
func generateAggregateRecord(numRecords int, content string) []byte {
	records := make([]types.Record, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		records = append(records, types.Record{
			PartitionKey: aws.String(fmt.Sprint(i)),
			Data:         []byte(content),
		})
	}
	return kpl.Aggregate(records)
}
