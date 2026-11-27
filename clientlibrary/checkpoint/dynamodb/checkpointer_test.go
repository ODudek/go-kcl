/*
 * Copyright (c) 2019 VMware, Inc.
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
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package dynamodb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
)

func TestDescribeTable(t *testing.T) {
	svc := &mockDynamoDB{client: nil, tableExist: true, item: map[string]types.AttributeValue{}}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		svc:       svc,
	}
	exists, usable, err := checkpoint.describeTable()
	assert.NoError(t, err)
	assert.True(t, exists, "Table exists but returned false")
	assert.True(t, usable)

	checkpoint.svc = &mockDynamoDB{tableExist: false}
	exists, usable, err = checkpoint.describeTable()
	assert.NoError(t, err)
	assert.False(t, exists, "Table does not exist but returned true")
	assert.False(t, usable)
}

func TestInit_RequiresADynamoDBClient(t *testing.T) {
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")

	assert.ErrorIs(t, NewDynamoCheckpoint(kclConfig).Init(), ErrNoDynamoDBClient)
}

// A DynamoCheckpoint is a checkpoint.Checkpointer.
var _ chk.Checkpointer = (*DynamoCheckpoint)(nil)

func TestGetLeaseNotAcquired(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         "0001",
		Checkpoint: "",
		Mux:        &sync.RWMutex{},
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	err = checkpoint.GetLease(&par.ShardStatus{
		ID:         "0001",
		Checkpoint: "",
		Mux:        &sync.RWMutex{},
	}, "ijkl-mnop")

	if err == nil || !errors.As(err, &chk.ErrLeaseNotAcquired{}) {
		t.Errorf("Got a lease when it was already held by abcd-efgh: %s", err)
	}
}

func TestGetLeaseAquired(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()
	marshalledCheckpoint := map[string]types.AttributeValue{
		chk.LeaseKeyKey: &types.AttributeValueMemberS{
			Value: "0001",
		},
		chk.LeaseOwnerKey: &types.AttributeValueMemberS{
			Value: "abcd-efgh",
		},
		chk.LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339),
		},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{
			Value: "deadbeef",
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	_, _ = checkpoint.svc.PutItem(context.TODO(), input)

	shard := &par.ShardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		Mux:        &sync.RWMutex{},
	}
	err := checkpoint.GetLease(shard, "ijkl-mnop")

	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
	}

	id, ok := svc.item[chk.SequenceNumberKey]
	if !ok {
		t.Error("Expected checkpoint to be set by GetLease")
	} else if id.(*types.AttributeValueMemberS).Value != "deadbeef" {
		t.Errorf("Expected checkpoint to be deadbeef. Got '%s'", id.(*types.AttributeValueMemberS).Value)
	}

	// release owner info
	err = checkpoint.RemoveLeaseOwner(shard.ID)
	assert.Nil(t, err)

	status := &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.RWMutex{},
	}
	_ = checkpoint.FetchCheckpoint(status)

	// checkpointer and parent shard id should be the same
	assert.Equal(t, shard.Checkpoint, status.Checkpoint)
	assert.Equal(t, shard.ParentShardId, status.ParentShardId)

	// Only the lease owner has been wiped out
	assert.Equal(t, "", status.GetLeaseOwner())
}

func TestGetLeaseShardClaimed(t *testing.T) {
	leaseTimeout := time.Now().Add(-100 * time.Second).UTC()
	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]types.AttributeValue{
			chk.ClaimRequestKey: &types.AttributeValueMemberS{Value: "ijkl-mnop"},
			chk.LeaseTimeoutKey: &types.AttributeValueMemberS{Value: leaseTimeout.Format(time.RFC3339)},
		},
	}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "abcd-efgh")
	if err == nil || err.Error() != chk.ErrShardClaimed {
		t.Errorf("Got a lease when it was already claimed by by ijkl-mnop: %s", err)
	}

	err = checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "ijkl-mnop")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}
}

func TestGetLeaseClaimRequestExpiredOwner(t *testing.T) {
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	// Not expired
	leaseTimeout := time.Now().
		Add(-time.Duration(kclConfig.LeaseStealingClaimTimeoutMillis) * time.Millisecond).
		Add(1 * time.Second).
		UTC()

	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]types.AttributeValue{
			chk.LeaseOwnerKey:   &types.AttributeValueMemberS{Value: "abcd-efgh"},
			chk.ClaimRequestKey: &types.AttributeValueMemberS{Value: "ijkl-mnop"},
			chk.LeaseTimeoutKey: &types.AttributeValueMemberS{Value: leaseTimeout.Format(time.RFC3339)},
		},
	}

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "abcd-efgh")
	if err == nil || err.Error() != chk.ErrShardClaimed {
		t.Errorf("Got a lease when it was already claimed by ijkl-mnop: %s", err)
	}
}

func TestGetLeaseClaimRequestExpiredClaimer(t *testing.T) {
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	// Not expired
	leaseTimeout := time.Now().
		Add(-time.Duration(kclConfig.LeaseStealingClaimTimeoutMillis) * time.Millisecond).
		Add(121 * time.Second).
		UTC()

	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]types.AttributeValue{
			chk.LeaseOwnerKey:   &types.AttributeValueMemberS{Value: "abcd-efgh"},
			chk.ClaimRequestKey: &types.AttributeValueMemberS{Value: "ijkl-mnop"},
			chk.LeaseTimeoutKey: &types.AttributeValueMemberS{Value: leaseTimeout.Format(time.RFC3339)},
		},
	}

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "ijkl-mnop")
	if err == nil || !errors.As(err, &chk.ErrLeaseNotAcquired{}) {
		t.Errorf("Got a lease when it was already claimed by ijkl-mnop: %s", err)
	}
}

func TestFetchCheckpointWithStealing(t *testing.T) {
	future := time.Now().AddDate(0, 1, 0)

	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]types.AttributeValue{
			chk.SequenceNumberKey: &types.AttributeValueMemberS{Value: "deadbeef"},
			chk.LeaseOwnerKey:     &types.AttributeValueMemberS{Value: "abcd-efgh"},
			chk.LeaseTimeoutKey: &types.AttributeValueMemberS{
				Value: future.Format(time.RFC3339),
			},
		},
	}

	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()

	status := &par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: time.Now(),
		Mux:          &sync.RWMutex{},
	}

	_ = checkpoint.FetchCheckpoint(status)

	leaseTimeout, _ := time.Parse(time.RFC3339, svc.item[chk.LeaseTimeoutKey].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, leaseTimeout, status.LeaseTimeout)
}

func TestGetLeaseConditional(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()
	marshalledCheckpoint := map[string]types.AttributeValue{
		chk.LeaseKeyKey: &types.AttributeValueMemberS{
			Value: "0001",
		},
		chk.LeaseOwnerKey: &types.AttributeValueMemberS{
			Value: "abcd-efgh",
		},
		chk.LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: time.Now().Add(-1 * time.Second).UTC().Format(time.RFC3339),
		},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{
			Value: "deadbeef",
		},
		chk.ClaimRequestKey: &types.AttributeValueMemberS{
			Value: "ijkl-mnop",
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	_, _ = checkpoint.svc.PutItem(context.TODO(), input)

	shard := &par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "deadbeef",
		ClaimRequest: "ijkl-mnop",
		Mux:          &sync.RWMutex{},
	}
	err := checkpoint.FetchCheckpoint(shard)
	if err != nil {
		t.Errorf("Could not fetch checkpoint %s", err)
	}

	err = checkpoint.GetLease(shard, "ijkl-mnop")
	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
	}
	assert.Equal(t, svc.expressionAttributeValues[":claim_request"].(*types.AttributeValueMemberS).Value, "ijkl-mnop")
	assert.Contains(t, svc.conditionalExpression, " AND ClaimRequest = :claim_request")
}

func TestListActiveWorkers(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	shardStatus := map[string]*par.ShardStatus{
		"0000": {ID: "0000", AssignedTo: "worker_1", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0001": {ID: "0001", AssignedTo: "worker_2", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0002": {ID: "0002", AssignedTo: "worker_4", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0003": {ID: "0003", AssignedTo: "worker_0", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0004": {ID: "0004", AssignedTo: "worker_1", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0005": {ID: "0005", AssignedTo: "worker_3", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0006": {ID: "0006", AssignedTo: "worker_3", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0007": {ID: "0007", AssignedTo: "worker_0", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0008": {ID: "0008", AssignedTo: "worker_4", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0009": {ID: "0009", AssignedTo: "worker_2", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0010": {ID: "0010", AssignedTo: "worker_0", Checkpoint: chk.ShardEnd, Mux: &sync.RWMutex{}},
	}

	workers, err := checkpoint.ListActiveWorkers(shardStatus)
	if err != nil {
		t.Error(err)
	}

	for workerID, shards := range workers {
		assert.Equal(t, 2, len(shards))
		for _, shard := range shards {
			assert.Equal(t, workerID, shard.AssignedTo)
		}
	}
}

// A shard leased by another worker that has not checkpointed yet must still
// show its owner, or this worker sees it as free.
func TestFetchCheckpointWithoutCheckpointStillReadsLeaseOwner(t *testing.T) {
	timeout := time.Now().UTC().Add(time.Minute).Truncate(time.Millisecond)
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{
		chk.LeaseKeyKey:     &types.AttributeValueMemberS{Value: "0000"},
		chk.LeaseOwnerKey:   &types.AttributeValueMemberS{Value: "other-worker"},
		chk.LeaseTimeoutKey: &types.AttributeValueMemberS{Value: timeout.Format(time.RFC3339Nano)},
	}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)

	shard := &par.ShardStatus{ID: "0000", Mux: &sync.RWMutex{}}
	err := checkpoint.FetchCheckpoint(shard)

	assert.Equal(t, chk.ErrSequenceIDNotFound, err)
	assert.Equal(t, "other-worker", shard.GetLeaseOwner())
	assert.True(t, timeout.Equal(shard.GetLeaseTimeout()))
}

// An unassigned shard is skipped, not an error: the worker's normal lease path
// picks it up, and rebalance decides on its own not to steal meanwhile.
func TestListActiveWorkersSkipsUnassignedShard(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	owned := &par.ShardStatus{ID: "0001", Mux: &sync.RWMutex{}}
	owned.SetLeaseOwner("abc")
	shardStatus := map[string]*par.ShardStatus{
		"0000": {ID: "0000", Mux: &sync.RWMutex{}},
		"0001": owned,
	}

	workers, err := checkpoint.ListActiveWorkers(shardStatus)
	assert.NoError(t, err)
	assert.Len(t, workers, 1)
	assert.Len(t, workers["abc"], 1)
}

func TestClaimShard(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	_ = checkpoint.Init()

	marshalledCheckpoint := map[string]types.AttributeValue{
		"ShardID": &types.AttributeValueMemberS{
			Value: "0001",
		},
		"AssignedTo": &types.AttributeValueMemberS{
			Value: "abcd-efgh",
		},
		"LeaseTimeout": &types.AttributeValueMemberS{
			Value: time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339),
		},
		"Checkpoint": &types.AttributeValueMemberS{
			Value: "deadbeef",
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	_, _ = checkpoint.svc.PutItem(context.TODO(), input)

	shard := &par.ShardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		Mux:        &sync.RWMutex{},
	}

	err := checkpoint.ClaimShard(shard, "ijkl-mnop")
	if err != nil {
		t.Errorf("Shard not claimed %s", err)
	}

	claimRequest, ok := svc.item[chk.ClaimRequestKey]
	if !ok {
		t.Error("Expected claimRequest to be set by ClaimShard")
	} else if claimRequest.(*types.AttributeValueMemberS).Value != "ijkl-mnop" {
		t.Errorf("Expected checkpoint to be ijkl-mnop. Got '%s'", claimRequest.(*types.AttributeValueMemberS).Value)
	}

	status := &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.RWMutex{},
	}
	_ = checkpoint.FetchCheckpoint(status)

	// asiggnedTo, checkpointer, and parent shard id should be the same
	assert.Equal(t, shard.AssignedTo, status.AssignedTo)
	assert.Equal(t, shard.Checkpoint, status.Checkpoint)
	assert.Equal(t, shard.ParentShardId, status.ParentShardId)
}

// The other workers count a worker as active while it holds a lease whose
// timeout is in the future (Worker.leaseShare), so a released shard has to
// carry an expired timeout, not only lose its owner: otherwise the worker that
// shut down keeps counting as active until the old timeout passes, and the
// shards it released stay unread for that long.
func TestRemoveLeaseOwnerExpiresTheLeaseTimeout(t *testing.T) {
	inAMinute := time.Now().UTC().Add(time.Minute)
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{
		chk.LeaseKeyKey:       &types.AttributeValueMemberS{Value: "0001"},
		chk.LeaseOwnerKey:     &types.AttributeValueMemberS{Value: "abc"},
		chk.LeaseTimeoutKey:   &types.AttributeValueMemberS{Value: inAMinute.Format(time.RFC3339Nano)},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{Value: "deadbeef"},
	}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)

	// Another worker's view of the shard while "abc" still holds it.
	other := &par.ShardStatus{ID: "0001", Mux: &sync.RWMutex{}}
	assert.NoError(t, checkpoint.FetchCheckpoint(other))
	assert.True(t, other.GetLeaseTimeout().After(time.Now()), "the lease is held")

	before := time.Now().UTC()
	assert.NoError(t, checkpoint.RemoveLeaseOwner("0001"))

	// Only this worker's own lease is released.
	assert.Equal(t, "AssignedTo = :assigned_to", aws.ToString(svc.updateInput.ConditionExpression))
	assert.Equal(t, &types.AttributeValueMemberS{Value: "abc"}, svc.updateInput.ExpressionAttributeValues[":assigned_to"])

	// The owner is gone, and the checkpoint is untouched.
	_, hasOwner := svc.item[chk.LeaseOwnerKey]
	assert.False(t, hasOwner)
	assert.Equal(t, "deadbeef", svc.item[chk.SequenceNumberKey].(*types.AttributeValueMemberS).Value)

	// The timeout is now, in the format FetchCheckpoint parses.
	assert.NoError(t, checkpoint.FetchCheckpoint(other))
	assert.False(t, other.GetLeaseTimeout().After(time.Now()), "the released lease must not count as held")
	assert.False(t, other.GetLeaseTimeout().Before(before), "the timeout is set at release time")
}

// ownerCheckingDynamoDB makes UpdateItem enforce the owner condition of
// RemoveLeaseOwner, as DynamoDB does.
type ownerCheckingDynamoDB struct{ *mockDynamoDB }

func (o ownerCheckingDynamoDB) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	owner, ok := o.item[chk.LeaseOwnerKey]
	want := params.ExpressionAttributeValues[":assigned_to"]
	if !ok || owner.(*types.AttributeValueMemberS).Value != want.(*types.AttributeValueMemberS).Value {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
	}
	return o.mockDynamoDB.UpdateItem(ctx, params, optFns...)
}

// A lease another worker holds by now is not ours to release: neither its
// owner nor its timeout may change.
func TestRemoveLeaseOwnerLeavesAnotherWorkersLeaseAlone(t *testing.T) {
	timeout := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	svc := &mockDynamoDB{tableExist: true, item: map[string]types.AttributeValue{
		chk.LeaseKeyKey:       &types.AttributeValueMemberS{Value: "0001"},
		chk.LeaseOwnerKey:     &types.AttributeValueMemberS{Value: "other-worker"},
		chk.LeaseTimeoutKey:   &types.AttributeValueMemberS{Value: timeout},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{Value: "deadbeef"},
	}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(ownerCheckingDynamoDB{svc})

	err := checkpoint.RemoveLeaseOwner("0001")

	var conditionFailed *types.ConditionalCheckFailedException
	assert.ErrorAs(t, err, &conditionFailed)
	assert.Equal(t, "other-worker", svc.item[chk.LeaseOwnerKey].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, timeout, svc.item[chk.LeaseTimeoutKey].(*types.AttributeValueMemberS).Value)
}

// tableDDB scripts the lease table lookups of Init: each DescribeTable call
// returns the next response (the last one repeats), and CreateTable returns
// createErr.
type tableDDB struct {
	*mockDynamoDB
	describe  []describeResponse
	createErr error
	describes int
	creates   int
}

type describeResponse struct {
	status types.TableStatus
	err    error
}

func (d *tableDDB) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	r := d.describe[min(d.describes, len(d.describe)-1)]
	d.describes++
	if r.err != nil {
		return nil, r.err
	}
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: r.status}}, nil
}

func (d *tableDDB) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	d.creates++
	return &dynamodb.CreateTableOutput{}, d.createErr
}

var (
	tableMissing  = describeResponse{err: &types.ResourceNotFoundException{Message: aws.String("Requested resource not found")}}
	tableCreating = describeResponse{status: types.TableStatusCreating}
	tableActive   = describeResponse{status: types.TableStatusActive}
	tableUpdating = describeResponse{status: types.TableStatusUpdating}
)

func newTableCheckpoint(t *testing.T, svc *tableDDB) *DynamoCheckpoint {
	t.Helper()
	if svc.mockDynamoDB == nil {
		svc.mockDynamoDB = &mockDynamoDB{item: map[string]types.AttributeValue{}}
	}
	timeout, minDelay, maxDelay := tableActiveTimeout, tableActiveMinDelay, tableActiveMaxDelay
	tableActiveTimeout, tableActiveMinDelay, tableActiveMaxDelay = 200*time.Millisecond, time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { tableActiveTimeout, tableActiveMinDelay, tableActiveMaxDelay = timeout, minDelay, maxDelay })

	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	return NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
}

func TestInit_ActiveTableIsUsedAsIs(t *testing.T) {
	svc := &tableDDB{describe: []describeResponse{tableActive}}

	assert.NoError(t, newTableCheckpoint(t, svc).Init())
	assert.Equal(t, 1, svc.describes)
	assert.Equal(t, 0, svc.creates)
}

// An UPDATING table (capacity, streams, an index or encryption changing) still
// serves reads and writes, so a worker starting meanwhile must not wait for
// the update to finish.
func TestInit_UpdatingTableIsUsedAsIs(t *testing.T) {
	svc := &tableDDB{describe: []describeResponse{tableUpdating}}

	assert.NoError(t, newTableCheckpoint(t, svc).Init())
	assert.Equal(t, 1, svc.describes)
	assert.Equal(t, 0, svc.creates)
}

func TestInit_CreatesAMissingTableAndWaitsUntilItIsActive(t *testing.T) {
	svc := &tableDDB{describe: []describeResponse{tableMissing, tableCreating, tableCreating, tableActive}}

	assert.NoError(t, newTableCheckpoint(t, svc).Init())
	assert.Equal(t, 1, svc.creates)
	assert.Equal(t, 4, svc.describes)
}

// Workers that start together on a new table all call CreateTable; the ones
// that lose get ResourceInUseException and must still start.
func TestInit_TableCreatedByAnotherWorkerIsNotAnError(t *testing.T) {
	svc := &tableDDB{
		describe:  []describeResponse{tableMissing, tableCreating, tableActive},
		createErr: &types.ResourceInUseException{Message: aws.String("Table already exists")},
	}

	assert.NoError(t, newTableCheckpoint(t, svc).Init())
	assert.Equal(t, 1, svc.creates)
}

// Right after another worker created the table, DescribeTable can still
// report it missing for a moment; the wait covers that.
func TestInit_WaitsForATableThatIsNotVisibleYet(t *testing.T) {
	svc := &tableDDB{
		describe:  []describeResponse{tableMissing, tableMissing, tableActive},
		createErr: &types.ResourceInUseException{Message: aws.String("Table already exists")},
	}

	assert.NoError(t, newTableCheckpoint(t, svc).Init())
}

func TestInit_WaitsForATableThatIsBeingCreated(t *testing.T) {
	svc := &tableDDB{describe: []describeResponse{tableCreating, tableCreating, tableActive}}

	assert.NoError(t, newTableCheckpoint(t, svc).Init())
	assert.Equal(t, 0, svc.creates, "a CREATING table exists and must not be created again")
	assert.Equal(t, 3, svc.describes, "Init returns only once the table is ACTIVE")
}

// A failing lookup is not a missing table: Init returns the real error and
// does not try to create a table that probably exists.
func TestInit_ReturnsDescribeTableErrors(t *testing.T) {
	cause := errors.New("operation error DynamoDB: DescribeTable, context deadline exceeded")
	svc := &tableDDB{describe: []describeResponse{{err: cause}}}

	err := newTableCheckpoint(t, svc).Init()

	require.ErrorIs(t, err, cause)
	assert.Contains(t, err.Error(), "appName", "the error names the lease table")
	assert.Equal(t, 0, svc.creates)
}

func TestInit_ReturnsCreateTableErrors(t *testing.T) {
	cause := errors.New("AccessDeniedException: not authorized to perform: dynamodb:CreateTable")
	svc := &tableDDB{describe: []describeResponse{tableMissing}, createErr: cause}

	assert.ErrorIs(t, newTableCheckpoint(t, svc).Init(), cause)
}

func TestInit_GivesUpWhenTheTableDoesNotBecomeActive(t *testing.T) {
	svc := &tableDDB{describe: []describeResponse{tableCreating}}

	err := newTableCheckpoint(t, svc).Init()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ACTIVE")
}

// replacingPutDynamoDB gives PutItem DynamoDB's semantics: the whole item is
// replaced, so attributes the caller doesn't send are deleted.
type replacingPutDynamoDB struct{ *mockDynamoDB }

func (r replacingPutDynamoDB) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	item := map[string]types.AttributeValue{}
	for k, v := range params.Item {
		item[k] = v
	}
	r.mockDynamoDB.item = item
	return &dynamodb.PutItemOutput{}, nil
}

// stringAttr returns a string attribute of item, or "" when it is missing.
func stringAttr(item map[string]types.AttributeValue, key string) string {
	if v, ok := item[key].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

// leaseItem is a lease row for shard 0001 at checkpoint seq-1, with a pending
// claim when claimer is set.
func leaseItem(owner, claimer, timeout string) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		chk.LeaseKeyKey:       &types.AttributeValueMemberS{Value: "0001"},
		chk.LeaseOwnerKey:     &types.AttributeValueMemberS{Value: owner},
		chk.LeaseTimeoutKey:   &types.AttributeValueMemberS{Value: timeout},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{Value: "seq-1"},
	}
	if claimer != "" {
		item[chk.ClaimRequestKey] = &types.AttributeValueMemberS{Value: claimer}
	}
	return item
}

// A checkpoint by the owner must not delete another worker's pending claim:
// otherwise every checkpoint the owner makes while a steal is in progress
// cancels the steal.
func TestCheckpointSequenceKeepsAPendingClaim(t *testing.T) {
	timeout := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	svc := &mockDynamoDB{tableExist: true, item: leaseItem("abc", "thief", timeout)}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(replacingPutDynamoDB{svc})

	shard := &par.ShardStatus{ID: "0001", Mux: &sync.RWMutex{}}
	shard.SetLeaseOwner("abc")
	shard.SetCheckpoint("seq-2")
	require.NoError(t, checkpoint.CheckpointSequence(shard))

	assert.Equal(t, "seq-2", stringAttr(svc.item, chk.SequenceNumberKey))
	assert.Equal(t, "thief", stringAttr(svc.item, chk.ClaimRequestKey), "the pending claim is kept")
	assert.Equal(t, "abc", stringAttr(svc.item, chk.LeaseOwnerKey))
	assert.Equal(t, timeout, stringAttr(svc.item, chk.LeaseTimeoutKey), "the lease timeout is left to GetLease")

	require.NotNil(t, svc.updateInput)
	assert.Equal(t, "AssignedTo = :assigned_to", aws.ToString(svc.updateInput.ConditionExpression))
	assert.Equal(t, &types.AttributeValueMemberS{Value: "abc"}, svc.updateInput.ExpressionAttributeValues[":assigned_to"])
}

// A worker that has lost the shard must not write its checkpoint over the new
// owner's, nor make itself the owner again.
func TestCheckpointSequenceRefusesALeaseHeldByAnotherWorker(t *testing.T) {
	timeout := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	svc := &mockDynamoDB{tableExist: true, item: leaseItem("new-owner", "", timeout)}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(ownerCheckingDynamoDB{svc})

	shard := &par.ShardStatus{ID: "0001", Mux: &sync.RWMutex{}}
	shard.SetLeaseOwner("abc")
	shard.SetCheckpoint("stale-seq")
	err := checkpoint.CheckpointSequence(shard)

	require.ErrorAs(t, err, &chk.ErrLeaseNotAcquired{})
	assert.Equal(t, "seq-1", stringAttr(svc.item, chk.SequenceNumberKey), "the new owner's checkpoint is kept")
	assert.Equal(t, "new-owner", stringAttr(svc.item, chk.LeaseOwnerKey))
}

func TestCheckpointSequenceWritesTheParentShardID(t *testing.T) {
	timeout := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	svc := &mockDynamoDB{tableExist: true, item: leaseItem("abc", "", timeout)}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc")
	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)

	shard := &par.ShardStatus{ID: "0001", ParentShardId: "0000", Mux: &sync.RWMutex{}}
	shard.SetLeaseOwner("abc")
	shard.SetCheckpoint(chk.ShardEnd)
	require.NoError(t, checkpoint.CheckpointSequence(shard))

	assert.Equal(t, chk.ShardEnd, stringAttr(svc.item, chk.SequenceNumberKey))
	assert.Equal(t, "0000", stringAttr(svc.item, chk.ParentShardIdKey))
}
