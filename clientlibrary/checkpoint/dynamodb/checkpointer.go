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

// Package dynamodb implements the checkpoint.Checkpointer interface on an
// Amazon DynamoDB lease table.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/ODudek/go-kcl/logger"
)

const (
	// NumMaxRetries is the max times of doing retry
	NumMaxRetries = 10
)

// DynamoCheckpoint implements the checkpoint.Checkpointer interface using AWS DynamoDB as the backend.
// It manages lease acquisition via conditional PutItem operations, checkpoint persistence,
// and optional lease stealing via claim requests.
type DynamoCheckpoint struct {
	log                     logger.Logger
	TableName               string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64

	LeaseDuration int
	svc           DynamoDBAPI
	kclConfig     *config.KinesisClientLibConfiguration
	Retries       int
	lastLeaseSync time.Time
}

// NewDynamoCheckpoint creates a DynamoDB-backed checkpoint.Checkpointer from the given configuration.
// Set its client with WithDynamoDB; Init then creates the lease table if needed.
func NewDynamoCheckpoint(kclConfig *config.KinesisClientLibConfiguration) *DynamoCheckpoint {
	checkpointer := &DynamoCheckpoint{
		log:                     kclConfig.Logger,
		TableName:               kclConfig.TableName,
		leaseTableReadCapacity:  int64(kclConfig.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(kclConfig.InitialLeaseTableWriteCapacity),
		LeaseDuration:           kclConfig.FailoverTimeMillis,
		kclConfig:               kclConfig,
		Retries:                 NumMaxRetries,
	}

	return checkpointer
}

// WithDynamoDB sets the DynamoDB client (required), e.g. a *dynamodb.Client
// or a mock. Returns the receiver for method chaining.
func (checkpointer *DynamoCheckpoint) WithDynamoDB(svc DynamoDBAPI) *DynamoCheckpoint {
	checkpointer.svc = svc
	return checkpointer
}

// ErrNoDynamoDBClient is returned by Init when no DynamoDB client was set with
// WithDynamoDB.
var ErrNoDynamoDBClient = errors.New("no DynamoDB client: set one with WithDynamoDB (awsclients.NewDynamoDBClient builds one from the KCL configuration)")

// Init ensures the lease table exists and can be used, through the DynamoDB
// client set with WithDynamoDB:
//   - Only ResourceNotFoundException from DescribeTable means the table is
//     missing; any other DescribeTable error is returned, instead of being
//     taken for a missing table and turned into a misleading CreateTable error.
//   - A missing table is created. ResourceInUseException from CreateTable means
//     another worker created it first, which is not an error: workers that
//     start together on a new table all get past Init.
//   - A table that was just created, or is still CREATING, is waited for until
//     it is ACTIVE (at most tableActiveTimeout), so the first lease reads and
//     writes do not race its creation. A table in any other state (ACTIVE, or
//     UPDATING, which still serves reads and writes) is used as is.
func (checkpointer *DynamoCheckpoint) Init() error {
	if checkpointer.svc == nil {
		return ErrNoDynamoDBClient
	}

	exists, usable, err := checkpointer.describeTable()
	if err != nil {
		return fmt.Errorf("describe lease table %s: %w", checkpointer.TableName, err)
	}
	if usable {
		return nil
	}

	if !exists {
		if err := checkpointer.createTable(); err != nil {
			var inUse *types.ResourceInUseException
			if !errors.As(err, &inUse) {
				return fmt.Errorf("create lease table %s: %w", checkpointer.TableName, err)
			}
			checkpointer.log.Infof("Lease table %s is being created by another worker", checkpointer.TableName)
		}
	}

	return checkpointer.waitUntilTableActive()
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	currentCheckpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	isClaimRequestExpired := shard.IsClaimRequestExpired(checkpointer.kclConfig)

	var claimRequest string
	if checkpointer.kclConfig.EnableLeaseStealing {
		if currentCheckpointClaimRequest, ok := currentCheckpoint[chk.ClaimRequestKey]; ok &&
			currentCheckpointClaimRequest.(*types.AttributeValueMemberS).Value != "" {
			claimRequest = currentCheckpointClaimRequest.(*types.AttributeValueMemberS).Value
			if newAssignTo != claimRequest && !isClaimRequestExpired {
				checkpointer.log.Debugf("another worker: %s has a claim on this shard. Not going to renew the lease", claimRequest)
				return errors.New(chk.ErrShardClaimed)
			}
		}
	}

	assignedVar, assignedToOk := currentCheckpoint[chk.LeaseOwnerKey]
	leaseVar, leaseTimeoutOk := currentCheckpoint[chk.LeaseTimeoutKey]

	var conditionalExpression string
	var expressionAttributeValues map[string]types.AttributeValue

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := assignedVar.(*types.AttributeValueMemberS).Value
		leaseTimeout := leaseVar.(*types.AttributeValueMemberS).Value

		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, leaseTimeout)
		if err != nil {
			return err
		}

		if checkpointer.kclConfig.EnableLeaseStealing {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo && !isClaimRequestExpired {
				return chk.ErrLeaseNotAcquired{Cause: "current lease timeout not yet expired"}
			}
		} else {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo {
				return chk.ErrLeaseNotAcquired{Cause: "current lease timeout not yet expired"}
			}
		}

		checkpointer.log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s, newAssignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo, newAssignTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{
				Value: shard.ID,
			},
			":assigned_to": &types.AttributeValueMemberS{
				Value: assignedTo,
			},
			":lease_timeout": &types.AttributeValueMemberS{
				Value: leaseTimeout,
			},
		}
	}

	marshalledCheckpoint := map[string]types.AttributeValue{
		chk.LeaseKeyKey: &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		chk.LeaseOwnerKey: &types.AttributeValueMemberS{
			Value: newAssignTo,
		},
		chk.LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: newLeaseTimeoutString,
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[chk.ParentShardIdKey] = &types.AttributeValueMemberS{
			Value: shard.ParentShardId,
		}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint != "" {
		marshalledCheckpoint[chk.SequenceNumberKey] = &types.AttributeValueMemberS{
			Value: checkpoint,
		}
	}

	if checkpointer.kclConfig.EnableLeaseStealing {
		if claimRequest != "" && claimRequest == newAssignTo && !isClaimRequestExpired {
			if expressionAttributeValues == nil {
				expressionAttributeValues = make(map[string]types.AttributeValue)
			}
			conditionalExpression = conditionalExpression + " AND ClaimRequest = :claim_request"
			expressionAttributeValues[":claim_request"] = &types.AttributeValueMemberS{
				Value: claimRequest,
			}
		}
	}

	err = checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		var conditionalCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckErr) {
			return chk.ErrLeaseNotAcquired{Cause: conditionalCheckErr.ErrorMessage()}
		}
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {
	leaseTimeout := shard.GetLeaseTimeout().UTC().Format(time.RFC3339Nano)
	marshalledCheckpoint := map[string]types.AttributeValue{
		chk.LeaseKeyKey: &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{
			Value: shard.GetCheckpoint(),
		},
		chk.LeaseOwnerKey: &types.AttributeValueMemberS{
			Value: shard.GetLeaseOwner(),
		},
		chk.LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: leaseTimeout,
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[chk.ParentShardIdKey] = &types.AttributeValueMemberS{Value: shard.ParentShardId}
	}

	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	// The lease is read before the checkpoint: a shard another worker has just
	// leased has an owner but no checkpoint yet, and must not look free here.
	if assignedTo, ok := checkpoint[chk.LeaseOwnerKey]; ok {
		shard.SetLeaseOwner(assignedTo.(*types.AttributeValueMemberS).Value)
	}

	// Use up-to-date leaseTimeout to avoid ConditionalCheckFailedException when claiming
	if leaseTimeout, ok := checkpoint[chk.LeaseTimeoutKey]; ok && leaseTimeout.(*types.AttributeValueMemberS).Value != "" {
		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, leaseTimeout.(*types.AttributeValueMemberS).Value)
		if err != nil {
			return err
		}
		shard.LeaseTimeout = currentLeaseTimeout
	}

	sequenceID, ok := checkpoint[chk.SequenceNumberKey]
	if !ok {
		return chk.ErrSequenceIDNotFound
	}

	checkpointer.log.Debugf("Retrieved Shard Iterator %s", sequenceID.(*types.AttributeValueMemberS).Value)
	shard.SetCheckpoint(sequenceID.(*types.AttributeValueMemberS).Value)

	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (checkpointer *DynamoCheckpoint) RemoveLeaseInfo(shardID string) error {
	err := checkpointer.removeItem(shardID)

	if err != nil {
		checkpointer.log.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
	} else {
		checkpointer.log.Infof("Lease info for shard: %s has been removed.", shardID)
	}

	return err
}

// RemoveLeaseOwner to remove lease owner for the shard entry
func (checkpointer *DynamoCheckpoint) RemoveLeaseOwner(shardID string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]types.AttributeValue{
			chk.LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
		UpdateExpression: aws.String("remove " + chk.LeaseOwnerKey),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":assigned_to": &types.AttributeValueMemberS{
				Value: checkpointer.kclConfig.WorkerID,
			},
		},
		ConditionExpression: aws.String("AssignedTo = :assigned_to"),
	}

	_, err := checkpointer.svc.UpdateItem(context.TODO(), input)

	return err
}

// GetLeaseOwner returns current lease owner of given shard in checkpoints table
func (checkpointer *DynamoCheckpoint) GetLeaseOwner(shardID string) (string, error) {
	currentCheckpoint, err := checkpointer.getItem(shardID)
	if err != nil {
		return "", err
	}

	assignedVar, assignedToOk := currentCheckpoint[chk.LeaseOwnerKey]

	if !assignedToOk {
		return "", chk.ErrNoLeaseOwner
	}

	return assignedVar.(*types.AttributeValueMemberS).Value, nil

}

// ListActiveWorkers returns a map of workers and their shards
func (checkpointer *DynamoCheckpoint) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	err := checkpointer.syncLeases(shardStatus)
	if err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == chk.ShardEnd {
			continue
		}

		// A free shard has no worker to list; the caller sees it in shardStatus.
		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			checkpointer.log.Debugf("Shard not assigned, skipping. ShardID: %s, WorkerID: %s", shard.ID, checkpointer.kclConfig.WorkerID)
			continue
		}

		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (checkpointer *DynamoCheckpoint) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := checkpointer.FetchCheckpoint(shard)
	if err != nil && err != chk.ErrSequenceIDNotFound {
		return err
	}
	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339Nano)

	conditionalExpression := `ShardID = :id AND LeaseTimeout = :lease_timeout AND attribute_not_exists(ClaimRequest)`
	expressionAttributeValues := map[string]types.AttributeValue{
		":id": &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		":lease_timeout": &types.AttributeValueMemberS{
			Value: leaseTimeoutString,
		},
	}

	marshalledCheckpoint := map[string]types.AttributeValue{
		chk.LeaseKeyKey: &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		chk.LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: leaseTimeoutString,
		},
		chk.SequenceNumberKey: &types.AttributeValueMemberS{
			Value: shard.Checkpoint,
		},
		chk.ClaimRequestKey: &types.AttributeValueMemberS{
			Value: claimID,
		},
	}

	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
		conditionalExpression += " AND attribute_not_exists(AssignedTo)"
	} else {
		marshalledCheckpoint[chk.LeaseOwnerKey] = &types.AttributeValueMemberS{Value: leaseOwner}
		conditionalExpression += "AND AssignedTo = :assigned_to"
		expressionAttributeValues[":assigned_to"] = &types.AttributeValueMemberS{Value: leaseOwner}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint == "" {
		conditionalExpression += " AND attribute_not_exists(Checkpoint)"
	} else if checkpoint == chk.ShardEnd {
		conditionalExpression += " AND Checkpoint <> :checkpoint"
		expressionAttributeValues[":checkpoint"] = &types.AttributeValueMemberS{Value: chk.ShardEnd}
	} else {
		conditionalExpression += " AND Checkpoint = :checkpoint"
		expressionAttributeValues[":checkpoint"] = &types.AttributeValueMemberS{Value: checkpoint}
	}

	if shard.ParentShardId == "" {
		conditionalExpression += " AND attribute_not_exists(ParentShardId)"
	} else {
		marshalledCheckpoint[chk.ParentShardIdKey] = &types.AttributeValueMemberS{Value: shard.ParentShardId}
		conditionalExpression += " AND ParentShardId = :parent_shard"
		expressionAttributeValues[":parent_shard"] = &types.AttributeValueMemberS{Value: shard.ParentShardId}
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
}

func (checkpointer *DynamoCheckpoint) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	log := checkpointer.kclConfig.Logger

	if (checkpointer.lastLeaseSync.Add(time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis) * time.Millisecond)).After(time.Now()) {
		return nil
	}

	checkpointer.lastLeaseSync = time.Now()
	input := &dynamodb.ScanInput{
		ProjectionExpression: aws.String(fmt.Sprintf("%s,%s,%s", chk.LeaseKeyKey, chk.LeaseOwnerKey, chk.SequenceNumberKey)),
		Select:               "SPECIFIC_ATTRIBUTES",
		TableName:            aws.String(checkpointer.kclConfig.TableName),
	}

	scanOutput, err := checkpointer.svc.Scan(context.TODO(), input)

	if err != nil {
		log.Debugf("Error performing DynamoDB Scan. Error: %+v ", err)
		return err
	}

	results := scanOutput.Items
	for _, result := range results {
		shardId, foundShardId := result[chk.LeaseKeyKey]
		assignedTo, foundAssignedTo := result[chk.LeaseOwnerKey]
		checkpoint, foundCheckpoint := result[chk.SequenceNumberKey]
		if !foundShardId || !foundAssignedTo || !foundCheckpoint {
			continue
		}

		if shard, ok := shardStatus[shardId.(*types.AttributeValueMemberS).Value]; ok {
			shard.SetLeaseOwner(assignedTo.(*types.AttributeValueMemberS).Value)
			shard.SetCheckpoint(checkpoint.(*types.AttributeValueMemberS).Value)
		}
	}

	log.Debugf("Lease sync completed. Next lease sync will occur in %s", time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis)*time.Millisecond)
	return nil
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(chk.LeaseKeyKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(chk.LeaseKeyKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(checkpointer.leaseTableReadCapacity),
			WriteCapacityUnits: aws.Int64(checkpointer.leaseTableWriteCapacity),
		},
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.CreateTable(context.Background(), input)

	return err
}

// describeTable reports whether the lease table exists and whether it can be
// used now. Only ResourceNotFoundException means it does not exist; any other
// error is returned. Only a CREATING table is not usable yet: an UPDATING table
// still serves reads and writes, and other states are left to the calls that
// follow, as before. A response without a table description is taken as an
// existing, usable table, since there is no status to wait for.
func (checkpointer *DynamoCheckpoint) describeTable() (exists, usable bool, err error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	out, err := checkpointer.svc.DescribeTable(context.Background(), input)
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return false, false, nil
		}
		return false, false, err
	}

	if out == nil || out.Table == nil {
		return true, true, nil
	}
	return true, out.Table.TableStatus != types.TableStatusCreating, nil
}

// How long Init waits for the lease table to become ACTIVE, and how often it
// checks. Variables so tests can shorten them.
var (
	tableActiveTimeout  = 2 * time.Minute
	tableActiveMinDelay = 1 * time.Second
	tableActiveMaxDelay = 5 * time.Second
)

// waitUntilTableActive waits until the lease table is ACTIVE. A table that is
// not visible yet (ResourceNotFoundException right after another worker
// created it) is waited for as well.
func (checkpointer *DynamoCheckpoint) waitUntilTableActive() error {
	checkpointer.log.Infof("Waiting for lease table %s to become ACTIVE", checkpointer.TableName)
	waiter := dynamodb.NewTableExistsWaiter(checkpointer.svc, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay = tableActiveMinDelay
		o.MaxDelay = tableActiveMaxDelay
	})
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	if err := waiter.Wait(context.Background(), input, tableActiveTimeout); err != nil {
		return fmt.Errorf("wait for lease table %s to become ACTIVE: %w", checkpointer.TableName, err)
	}
	return nil
}

func (checkpointer *DynamoCheckpoint) saveItem(item map[string]types.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(checkpointer.TableName),
		Item:      item,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]types.AttributeValue, item map[string]types.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(checkpointer.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) putItem(input *dynamodb.PutItemInput) error {
	_, err := checkpointer.svc.PutItem(context.Background(), input)
	return err
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]types.AttributeValue, error) {
	item, err := checkpointer.svc.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(checkpointer.TableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			chk.LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	})

	// fix problem when starts the environment from scratch (dynamo table is empty)
	if item == nil {
		return nil, err
	}

	return item.Item, err
}

func (checkpointer *DynamoCheckpoint) removeItem(shardID string) error {
	_, err := checkpointer.svc.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]types.AttributeValue{
			chk.LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	})

	return err
}
