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

// Package interfaces defines the user-facing interfaces for the Kinesis Client Library.
//
// Applications must implement IRecordProcessor and IRecordProcessorFactory to
// consume records from a Kinesis stream. The library manages the lifecycle:
// Initialize -> ProcessRecords (repeated) -> Shutdown.
package interfaces

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const (
	// REQUESTED indicates the entire application is shutting down.
	// The record processor will be given a final chance to checkpoint.
	REQUESTED ShutdownReason = iota + 1

	// TERMINATE indicates the shard is closed and all records have been delivered.
	// Applications MUST checkpoint their progress so that processing of child
	// shards can begin.
	TERMINATE

	// ZOMBIE indicates processing is moving to a different record processor
	// (failover or load balancing). Applications SHOULD NOT checkpoint as another
	// processor may have already started processing data.
	ZOMBIE
)

type (
	// ShutdownReason indicates why a record processor is being shut down.
	// Used to distinguish between failover vs. termination (shard closed).
	// In case of failover, applications should NOT checkpoint.
	// In case of termination, applications SHOULD checkpoint their progress.
	ShutdownReason int

	// InitializationInput provides information to the record processor when it
	// starts processing a new shard. Passed to IRecordProcessor.Initialize.
	InitializationInput struct {
		// ShardId is the unique identifier of the shard this processor is responsible for.
		ShardId string

		// ExtendedSequenceNumber is the last successfully checkpointed position.
		// Nil indicates no prior checkpoint exists.
		ExtendedSequenceNumber *ExtendedSequenceNumber
	}

	// ProcessRecordsInput contains a batch of records from a Kinesis shard along
	// with metadata and a checkpointer for tracking progress. Passed to
	// IRecordProcessor.ProcessRecords.
	ProcessRecordsInput struct {
		// CacheEntryTime is when the batch was received from Kinesis.
		CacheEntryTime *time.Time

		// CacheExitTime is when the batch was prepared for delivery to the processor.
		CacheExitTime *time.Time

		// Records are the data records from Kinesis, potentially de-aggregated
		// if published by the KPL.
		Records []types.Record

		// Checkpointer provides methods to checkpoint progress within this batch.
		Checkpointer IRecordProcessorCheckpointer

		// MillisBehindLatest is how far behind the stream tip this batch was when
		// received, in milliseconds. Use this to monitor processing lag.
		MillisBehindLatest int64
	}

	// ShutdownInput provides information and capabilities when a record processor
	// is being shut down. Passed to IRecordProcessor.Shutdown.
	ShutdownInput struct {
		// ShutdownReason indicates why this processor is shutting down.
		ShutdownReason ShutdownReason

		// Checkpointer provides methods to record final progress before shutdown.
		Checkpointer IRecordProcessorCheckpointer
	}
)

var shutdownReasonMap = map[ShutdownReason]*string{
	REQUESTED: aws.String("REQUESTED"),
	TERMINATE: aws.String("TERMINATE"),
	ZOMBIE:    aws.String("ZOMBIE"),
}

// ShutdownReasonMessage returns a human-readable string for the given ShutdownReason
// (e.g. "TERMINATE", "ZOMBIE", "REQUESTED").
func ShutdownReasonMessage(reason ShutdownReason) *string {
	return shutdownReasonMap[reason]
}
