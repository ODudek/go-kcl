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

package config

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/ODudek/go-kcl/clientlibrary/metrics"
	"github.com/ODudek/go-kcl/clientlibrary/utils"
	"github.com/ODudek/go-kcl/logger"
)

// NewKinesisClientLibConfig creates a default KinesisClientLibConfiguration based on the required fields.
func NewKinesisClientLibConfig(applicationName, streamName, regionName, workerID string) *KinesisClientLibConfiguration {
	return NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID,
		nil, nil)
}

// NewKinesisClientLibConfigWithCredential creates a default KinesisClientLibConfiguration based on the required fields and unique credentials.
func NewKinesisClientLibConfigWithCredential(applicationName, streamName, regionName, workerID string,
	creds aws.CredentialsProvider) *KinesisClientLibConfiguration {
	return NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID, creds, creds)
}

// NewKinesisClientLibConfigWithCredentials creates a default KinesisClientLibConfiguration based on the required fields and specific credentials for each service.
func NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID string,
	kinesisCreds, dynamodbCreds aws.CredentialsProvider) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("ApplicationName", applicationName)
	checkIsValueNotEmpty("StreamName", streamName)
	checkIsValueNotEmpty("RegionName", regionName)

	if empty(workerID) {
		workerID = utils.MustNewUUID()
	}

	// populate the KCL configuration with default values
	return &KinesisClientLibConfiguration{
		ApplicationName:                                  applicationName,
		KinesisCredentials:                               kinesisCreds,
		DynamoDBCredentials:                              dynamodbCreds,
		TableName:                                        applicationName,
		EnhancedFanOutConsumerName:                       applicationName,
		StreamName:                                       streamName,
		RegionName:                                       regionName,
		WorkerID:                                         workerID,
		InitialPositionInStream:                          DefaultInitialPositionInStream,
		InitialPositionInStreamExtended:                  *newInitialPosition(DefaultInitialPositionInStream),
		FailoverTimeMillis:                               DefaultFailoverTimeMillis,
		LeaseRefreshPeriodMillis:                         DefaultLeaseRefreshPeriodMillis,
		MaxRecords:                                       DefaultMaxRecords,
		IdleTimeBetweenReadsInMillis:                     DefaultIdleTimeBetweenReadsMillis,
		CallProcessRecordsEvenForEmptyRecordList:         DefaultDontCallProcessRecordsForEmptyRecordList,
		ParentShardPollIntervalMillis:                    DefaultParentShardPollIntervalMillis,
		ShardSyncIntervalMillis:                          DefaultShardSyncIntervalMillis,
		CleanupTerminatedShardsBeforeExpiry:              DefaultCleanupLeasesUponShardsCompletion,
		TaskBackoffTimeMillis:                            DefaultTaskBackoffTimeMillis,
		ValidateSequenceNumberBeforeCheckpointing:        DefaultValidateSequenceNumberBeforeCheckpointing,
		ShutdownGraceMillis:                              DefaultShutdownGraceMillis,
		MaxLeasesForWorker:                               DefaultMaxLeasesForWorker,
		MaxLeasesToStealAtOneTime:                        DefaultMaxLeasesToStealAtOneTime,
		InitialLeaseTableReadCapacity:                    DefaultInitialLeaseTableReadCapacity,
		InitialLeaseTableWriteCapacity:                   DefaultInitialLeaseTableWriteCapacity,
		SkipShardSyncAtWorkerInitializationIfLeasesExist: DefaultSkipShardSyncAtStartupIfLeasesExist,
		EnableLeaseStealing:                              DefaultEnableLeaseStealing,
		LeaseStealingIntervalMillis:                      DefaultLeaseStealingIntervalMillis,
		LeaseStealingClaimTimeoutMillis:                  DefaultLeaseStealingClaimTimeoutMillis,
		LeaseSyncingTimeIntervalMillis:                   DefaultLeaseSyncingIntervalMillis,
		LeaseRefreshWaitTime:                             DefaultLeaseRefreshWaitTime,
		MaxRetryCount:                                    DefaultMaxRetryCount,
		Logger:                                           logger.GetDefaultLogger(),
	}
}

// WithKinesisEndpoint is used to provide an alternative Kinesis endpoint
func (c *KinesisClientLibConfiguration) WithKinesisEndpoint(kinesisEndpoint string) *KinesisClientLibConfiguration {
	c.KinesisEndpoint = kinesisEndpoint
	return c
}

// WithDynamoDBEndpoint is used to provide an alternative DynamoDB endpoint
func (c *KinesisClientLibConfiguration) WithDynamoDBEndpoint(dynamoDBEndpoint string) *KinesisClientLibConfiguration {
	c.DynamoDBEndpoint = dynamoDBEndpoint
	return c
}

// WithTableName sets an alternative DynamoDB/Redis table name for lease management.
// Defaults to ApplicationName.
func (c *KinesisClientLibConfiguration) WithTableName(tableName string) *KinesisClientLibConfiguration {
	c.TableName = tableName
	return c
}

// WithInitialPositionInStream sets where to start reading when no checkpoint exists.
// Valid values: LATEST, TRIM_HORIZON, or AT_TIMESTAMP.
func (c *KinesisClientLibConfiguration) WithInitialPositionInStream(initialPositionInStream InitialPositionInStream) *KinesisClientLibConfiguration {
	c.InitialPositionInStream = initialPositionInStream
	c.InitialPositionInStreamExtended = *newInitialPosition(initialPositionInStream)
	return c
}

// WithTimestampAtInitialPositionInStream sets the initial position to AT_TIMESTAMP with the given time.
func (c *KinesisClientLibConfiguration) WithTimestampAtInitialPositionInStream(timestamp *time.Time) *KinesisClientLibConfiguration {
	c.InitialPositionInStream = AT_TIMESTAMP
	c.InitialPositionInStreamExtended = *newInitialPositionAtTimestamp(timestamp)
	return c
}

// WithFailoverTimeMillis sets the lease duration in milliseconds. Workers that do not renew
// their lease within this period will have their shards reassigned. Default: 10000.
func (c *KinesisClientLibConfiguration) WithFailoverTimeMillis(failoverTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis)
	c.FailoverTimeMillis = failoverTimeMillis
	return c
}

// WithLeaseRefreshPeriodMillis sets the period before lease expiry during which the owner
// will attempt to renew the lease. Default: 5000.
func (c *KinesisClientLibConfiguration) WithLeaseRefreshPeriodMillis(leaseRefreshPeriodMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("LeaseRefreshPeriodMillis", leaseRefreshPeriodMillis)
	c.LeaseRefreshPeriodMillis = leaseRefreshPeriodMillis
	return c
}

// WithLeaseRefreshWaitTime sets the wait period in milliseconds before an async lease renewal. Default: 2500.
func (c *KinesisClientLibConfiguration) WithLeaseRefreshWaitTime(leaseRefreshWaitTime int) *KinesisClientLibConfiguration {
	checkIsValuePositive("LeaseRefreshWaitTime", leaseRefreshWaitTime)
	c.LeaseRefreshWaitTime = leaseRefreshWaitTime
	return c
}

// WithShardSyncIntervalMillis sets the interval in milliseconds between shard sync tasks. Default: 60000.
func (c *KinesisClientLibConfiguration) WithShardSyncIntervalMillis(shardSyncIntervalMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis)
	c.ShardSyncIntervalMillis = shardSyncIntervalMillis
	return c
}

// WithMaxRecords sets the maximum number of records per GetRecords call.
// The Kinesis API enforces an upper limit of 10000. Default: 10000.
func (c *KinesisClientLibConfiguration) WithMaxRecords(maxRecords int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxRecords", maxRecords)
	if maxRecords > MaxMaxRecords {
		log.Panicf("MaxRecords must not exceed %d (Kinesis API limit), got: %d", MaxMaxRecords, maxRecords)
	}
	c.MaxRecords = maxRecords
	return c
}

// WithMaxLeasesForWorker configures maximum lease this worker can handles. It determines how maximun number of shards
// this worker can handle.
func (c *KinesisClientLibConfiguration) WithMaxLeasesForWorker(n int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxLeasesForWorker", n)
	c.MaxLeasesForWorker = n
	return c
}

// WithIdleTimeBetweenReadsInMillis sets how long the consumer sleeps when no records are returned.
// This value is only used when no records are returned; when records are present, the next
// batch is fetched immediately. Setting this too high may cause the consumer to fall behind.
// Default: 1000.
func (c *KinesisClientLibConfiguration) WithIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis)
	c.IdleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis
	return c
}

// WithCallProcessRecordsEvenForEmptyRecordList controls whether ProcessRecords is called
// even when GetRecords returns no records. Default: false.
func (c *KinesisClientLibConfiguration) WithCallProcessRecordsEvenForEmptyRecordList(callProcessRecordsEvenForEmptyRecordList bool) *KinesisClientLibConfiguration {
	c.CallProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList
	return c
}

// WithTaskBackoffTimeMillis sets the backoff time in milliseconds when tasks encounter errors. Default: 500.
func (c *KinesisClientLibConfiguration) WithTaskBackoffTimeMillis(taskBackoffTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis)
	c.TaskBackoffTimeMillis = taskBackoffTimeMillis
	return c
}

// WithLogger sets a custom logger. Panics if nil.
func (c *KinesisClientLibConfiguration) WithLogger(logger logger.Logger) *KinesisClientLibConfiguration {
	if logger == nil {
		log.Panic("Logger cannot be null")
	}
	c.Logger = logger
	return c
}

// WithMaxRetryCount sets the max retry count in case of error.
func (c *KinesisClientLibConfiguration) WithMaxRetryCount(maxRetryCount int) *KinesisClientLibConfiguration {
	checkIsValuePositive("maxRetryCount", maxRetryCount)
	c.MaxRetryCount = maxRetryCount
	return c
}

// WithMonitoringService sets the monitoring service to use to publish metrics.
func (c *KinesisClientLibConfiguration) WithMonitoringService(mService metrics.MonitoringService) *KinesisClientLibConfiguration {
	// Nil case is handled downward (at worker creation) so no need to do it here.
	// Plus the user might want to be explicit about passing a nil monitoring service here.
	c.MonitoringService = mService
	return c
}

// WithEnhancedFanOutConsumer sets EnableEnhancedFanOutConsumer. If enhanced fan-out is enabled and ConsumerName is not specified ApplicationName is used as ConsumerName.
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
// Note: You can register up to twenty consumers per stream to use enhanced fan-out.
func (c *KinesisClientLibConfiguration) WithEnhancedFanOutConsumer(enable bool) *KinesisClientLibConfiguration {
	c.EnableEnhancedFanOutConsumer = enable
	return c
}

// WithEnhancedFanOutConsumerName enables enhanced fan-out consumer with the specified name
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
// Note: You can register up to twenty consumers per stream to use enhanced fan-out.
func (c *KinesisClientLibConfiguration) WithEnhancedFanOutConsumerName(consumerName string) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("EnhancedFanOutConsumerName", consumerName)
	c.EnhancedFanOutConsumerName = consumerName
	c.EnableEnhancedFanOutConsumer = true
	return c
}

// WithEnhancedFanOutConsumerARN enables enhanced fan-out consumer with the specified consumer ARN
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
// Note: You can register up to twenty consumers per stream to use enhanced fan-out.
func (c *KinesisClientLibConfiguration) WithEnhancedFanOutConsumerARN(consumerARN string) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("EnhancedFanOutConsumerARN", consumerARN)
	c.EnhancedFanOutConsumerARN = consumerARN
	c.EnableEnhancedFanOutConsumer = true
	return c
}

// WithLeaseStealing enables or disables lease stealing for load balancing across workers. Default: false.
func (c *KinesisClientLibConfiguration) WithLeaseStealing(enableLeaseStealing bool) *KinesisClientLibConfiguration {
	c.EnableLeaseStealing = enableLeaseStealing
	return c
}

// WithLeaseStealingIntervalMillis sets the interval between lease stealing (rebalancing) attempts. Default: 5000.
func (c *KinesisClientLibConfiguration) WithLeaseStealingIntervalMillis(leaseStealingIntervalMillis int) *KinesisClientLibConfiguration {
	c.LeaseStealingIntervalMillis = leaseStealingIntervalMillis
	return c
}

// WithLeaseSyncingIntervalMillis sets the interval before syncing with the lease table. Default: 60000.
func (c *KinesisClientLibConfiguration) WithLeaseSyncingIntervalMillis(leaseSyncingIntervalMillis int) *KinesisClientLibConfiguration {
	c.LeaseSyncingTimeIntervalMillis = leaseSyncingIntervalMillis
	return c
}
