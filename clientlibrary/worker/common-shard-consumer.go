/*
 * Copyright (c) 2021 VMware, Inc.
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

package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	kcl "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	"github.com/ODudek/go-kcl/clientlibrary/metrics"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/ODudek/go-kcl/internal/kpl"
)

type shardConsumer interface {
	getRecords() error
}

// KinesisSubscriberGetter abstracts the Kinesis API operations required by shard consumers.
// It enables dependency injection for testing and custom AWS configurations.
type KinesisSubscriberGetter interface {
	SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

// commonShardConsumer implements common functionality for regular and enhanced fan-out consumers
type commonShardConsumer struct {
	shard           *par.ShardStatus
	kc              KinesisSubscriberGetter
	checkpointer    chk.Checkpointer
	recordProcessor kcl.IRecordProcessor
	kclConfig       *config.KinesisClientLibConfiguration
	mService        metrics.MonitoringService
}

// Cleanup the internal lease cache
func (sc *commonShardConsumer) releaseLease(shard string) {
	log := sc.kclConfig.Logger
	log.Infof("Release lease for shard %s", sc.shard.ID)
	sc.shard.SetLeaseOwner("")

	// Release the lease by wiping out the lease owner for the shard. A lease
	// another worker holds by now is not ours to release; any other failure
	// leaves the shard leased to this worker until the lease expires or the
	// worker leases it again.
	if err := sc.checkpointer.RemoveLeaseOwner(sc.shard.ID); err != nil {
		if errors.As(err, &chk.ErrLeaseNotAcquired{}) {
			log.Debugf("Failed to release shard lease or shard: %s Error: %+v", sc.shard.ID, err)
		} else {
			log.Warnf("Failed to release lease on shard: %s, it stays leased to this worker until it expires. Error: %+v", sc.shard.ID, err)
		}
	}

	// reporting lease lose metrics
	sc.mService.DeleteMetricMillisBehindLatest(shard)
	sc.mService.LeaseLost(sc.shard.ID)
}

// canRetryLeaseRenewal reports whether a failed lease renewal can be tried
// again at retryAt instead of giving the shard up: the lease was not lost to
// another worker, and is still held at retryAt. A short outage of the
// checkpoint store then no longer stops the consumer, releases the lease and
// makes the shard be reprocessed from its last checkpoint.
func (sc *commonShardConsumer) canRetryLeaseRenewal(err error, retryAt time.Time) bool {
	if errors.As(err, &chk.ErrLeaseNotAcquired{}) || err.Error() == chk.ErrShardClaimed {
		return false
	}
	return retryAt.Before(sc.shard.GetLeaseTimeout())
}

// getStartingPosition gets kinesis stating position.
// First try to fetch checkpoint. If checkpoint is not found use InitialPositionInStream
func (sc *commonShardConsumer) getStartingPosition() (*types.StartingPosition, error) {
	err := sc.checkpointer.FetchCheckpoint(sc.shard)
	if err != nil && err != chk.ErrSequenceIDNotFound {
		return nil, err
	}

	checkpoint := sc.shard.GetCheckpoint()
	if checkpoint != "" {
		sc.kclConfig.Logger.Debugf("Start shard: %v at checkpoint: %v", sc.shard.ID, checkpoint)
		return &types.StartingPosition{
			Type:           types.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber: &checkpoint,
		}, nil
	}

	shardIteratorType := config.InitalPositionInStreamToShardIteratorType(sc.kclConfig.InitialPositionInStream)
	sc.kclConfig.Logger.Debugf("No checkpoint recorded for shard: %v, starting with: %v", sc.shard.ID, aws.ToString(shardIteratorType))
	if sc.kclConfig.InitialPositionInStream == config.AT_TIMESTAMP {
		return &types.StartingPosition{
			Type:      types.ShardIteratorTypeAtTimestamp,
			Timestamp: sc.kclConfig.InitialPositionInStreamExtended.Timestamp,
		}, nil
	}

	if *shardIteratorType == "TRIM_HORIZON" {
		return &types.StartingPosition{
			Type: types.ShardIteratorTypeTrimHorizon,
		}, nil
	}

	return &types.StartingPosition{
		Type: types.ShardIteratorTypeLatest,
	}, nil
}

// Need to wait until the parent shard finished
func (sc *commonShardConsumer) waitOnParentShard() error {
	if len(sc.shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &par.ShardStatus{
		ID:  sc.shard.ParentShardId,
		Mux: &sync.RWMutex{},
	}

	for {
		if err := sc.checkpointer.FetchCheckpoint(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.GetCheckpoint() == chk.ShardEnd {
			return nil
		}

		time.Sleep(time.Duration(sc.kclConfig.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

func (sc *commonShardConsumer) processRecords(getRecordsStartTime time.Time, records []types.Record, millisBehindLatest *int64, recordCheckpointer kcl.IRecordProcessorCheckpointer) error {
	log := sc.kclConfig.Logger

	getRecordsTime := time.Since(getRecordsStartTime).Milliseconds()
	sc.mService.RecordGetRecordsTime(sc.shard.ID, float64(getRecordsTime))

	log.Debugf("Received %d original records.", len(records))

	// De-aggregate the records if they were published by the KPL. A malformed
	// aggregated record is passed to the record processor unchanged.
	dars, err := kpl.Deaggregate(records)
	if err != nil {
		log.Errorf("Error in de-aggregating KPL records from shard %s: %+v", sc.shard.ID, err)
	}

	input := &kcl.ProcessRecordsInput{
		Records:            dars,
		MillisBehindLatest: *millisBehindLatest,
		Checkpointer:       recordCheckpointer,
	}

	recordLength := len(input.Records)
	recordBytes := int64(0)
	log.Debugf("Received %d de-aggregated records, MillisBehindLatest: %v", recordLength, input.MillisBehindLatest)

	for _, r := range input.Records {
		recordBytes += int64(len(r.Data))
	}

	if recordLength > 0 || sc.kclConfig.CallProcessRecordsEvenForEmptyRecordList {
		processRecordsStartTime := time.Now()

		// Deliver the events to the record processor.
		// If the processor returns an error, propagate it so the shard consumer
		// does not advance the iterator — the same batch will be retried from checkpoint.
		input.CacheEntryTime = &getRecordsStartTime
		input.CacheExitTime = &processRecordsStartTime
		if err := sc.recordProcessor.ProcessRecords(input); err != nil {
			return err
		}
		processedRecordsTiming := time.Since(processRecordsStartTime).Milliseconds()
		sc.mService.RecordProcessRecordsTime(sc.shard.ID, float64(processedRecordsTiming))
	}

	sc.mService.IncrRecordsProcessed(sc.shard.ID, recordLength)
	sc.mService.IncrBytesProcessed(sc.shard.ID, recordBytes)
	sc.mService.MillisBehindLatest(sc.shard.ID, float64(*millisBehindLatest))
	return nil
}
