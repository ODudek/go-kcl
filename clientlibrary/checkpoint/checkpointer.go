/*
 * Copyright (c) 2018 VMware, Inc.
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

// Package checkpoint provides interfaces and implementations for managing Kinesis
// shard leases and checkpoint tracking across distributed workers.
//
// The Checkpointer interface abstracts the underlying storage backend (DynamoDB, Redis)
// for persisting shard lease ownership, sequence number checkpoints, and lease
// claim requests. Implementations provide atomic conditional updates for consistency
// in a multi-worker environment.
//
// The implementation is derived from https://github.com/patrobinson/gokini
package checkpoint

import (
	"errors"
	"fmt"

	par "github.com/ODudek/go-kcl/clientlibrary/partition"
)

const (
	// LeaseKeyKey is the field name for the shard ID (primary key).
	LeaseKeyKey = "ShardID"
	// LeaseOwnerKey is the field name for the worker that owns the lease.
	LeaseOwnerKey = "AssignedTo"
	// LeaseTimeoutKey is the field name for the lease expiration time.
	LeaseTimeoutKey = "LeaseTimeout"
	// SequenceNumberKey is the field name for the last checkpointed sequence number.
	SequenceNumberKey = "Checkpoint"
	// ParentShardIdKey is the field name for the parent shard ID (resharding).
	ParentShardIdKey = "ParentShardId"
	// ClaimRequestKey is the field name for the worker claiming the shard (lease stealing).
	ClaimRequestKey = "ClaimRequest"

	// ShardEnd is the sentinel checkpoint value indicating a shard has been completely
	// processed and all records delivered.
	ShardEnd = "SHARD_END"

	// ErrShardClaimed is the error message returned when a lease acquisition fails
	// because another worker has an active claim on the shard.
	ErrShardClaimed = "shard is already claimed by another node"
)

// ErrLeaseNotAcquired is returned when a worker cannot acquire a lease on a shard,
// typically because another worker holds an active lease or a claim request is in progress.
type ErrLeaseNotAcquired struct {
	Cause string
}

func (e ErrLeaseNotAcquired) Error() string {
	return fmt.Sprintf("lease not acquired: %s", e.Cause)
}

// Checkpointer manages shard lease acquisition, renewal, checkpointing, and lease stealing.
// Implementations must provide atomic conditional updates to ensure consistency
// across multiple concurrent workers.
type Checkpointer interface {
	// Init establishes a connection to the backend store and creates the lease table if needed.
	Init() error

	// GetLease attempts to acquire or renew a lease on the given shard for the specified worker.
	GetLease(*par.ShardStatus, string) error

	// CheckpointSequence persists the current checkpoint sequence number for the shard.
	CheckpointSequence(*par.ShardStatus) error

	// FetchCheckpoint retrieves the stored checkpoint, lease owner, and lease timeout for the shard.
	FetchCheckpoint(*par.ShardStatus) error

	// RemoveLeaseInfo removes all lease data for a shard that no longer exists in Kinesis.
	RemoveLeaseInfo(string) error

	// RemoveLeaseOwner clears the lease owner for a shard, making it available for reassignment.
	RemoveLeaseOwner(string) error

	// GetLeaseOwner returns the current lease owner for the specified shard.
	GetLeaseOwner(string) (string, error)

	// ListActiveWorkers returns a map of worker IDs to their assigned shards (used for rebalancing).
	ListActiveWorkers(map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error)

	// ClaimShard places a claim request on a shard to signal a steal attempt.
	ClaimShard(*par.ShardStatus, string) error
}

// ErrSequenceIDNotFound is returned by FetchCheckpoint when no SequenceID is found
var ErrSequenceIDNotFound = errors.New("SequenceIDNotFoundForShard")

// ErrShardNotAssigned is returned by ListActiveWorkers when no AssignedTo is found
var ErrShardNotAssigned = errors.New("AssignedToNotFoundForShard")

// ErrNoLeaseOwner is returned by GetLeaseOwner when no lease owner exists for the shard
var ErrNoLeaseOwner = errors.New("no LeaseOwner in checkpoints table")
