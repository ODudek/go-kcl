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

// Package worker implements the Worker orchestrator for consuming Amazon Kinesis data streams.
//
// The Worker coordinates shard discovery, lease management, and record processing.
// It spins up goroutine-based consumers for each assigned shard using either polling
// (GetRecords API) or enhanced fan-out (SubscribeToShard API) strategies.
//
// The implementation is derived from https://github.com/patrobinson/gokini
package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	kcl "github.com/ODudek/go-kcl/clientlibrary/interfaces"
	"github.com/ODudek/go-kcl/clientlibrary/metrics"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
)

// Worker is the high level class that Kinesis applications use to start processing data. It initializes and oversees
// different components (e.g. syncing shard and lease information, tracking shard assignments, and processing data from
// the shards).
type Worker struct {
	streamName  string
	workerID    string
	consumerARN string

	processorFactory kcl.IRecordProcessorFactory
	kclConfig        *config.KinesisClientLibConfiguration
	kc               *kinesis.Client
	checkpointer     chk.Checkpointer
	mService         metrics.MonitoringService

	stop      *chan struct{}
	waitGroup *sync.WaitGroup
	done      bool

	randomSeed int64

	shardStatus          map[string]*par.ShardStatus
	shardStealInProgress bool

	// consumers holds the IDs of the shards a consumer goroutine is reading.
	// It, not the lease owner in shardStatus, tells which shards this worker
	// is consuming: the owner is also read back from the checkpointer, which
	// can still name this worker after a consumer exited and failed to release
	// its lease.
	consumersMu sync.Mutex
	consumers   map[string]bool
}

var (
	// ErrNoKinesisClient is returned by Start when no Kinesis client was set
	// with WithKinesis.
	ErrNoKinesisClient = errors.New("no Kinesis client: set one with WithKinesis (awsclients.NewKinesisClient builds one from the KCL configuration)")
	// ErrNoCheckpointer is returned by Start when no checkpointer was set with
	// WithCheckpointer.
	ErrNoCheckpointer = errors.New("no checkpointer: set one with WithCheckpointer (checkpoint/redis or checkpoint/dynamodb)")
)

// NewWorker constructs a Worker instance for processing Kinesis stream data.
// Set its Kinesis client with WithKinesis and its checkpointer with
// WithCheckpointer before calling Start.
func NewWorker(factory kcl.IRecordProcessorFactory, kclConfig *config.KinesisClientLibConfiguration) *Worker {
	mService := kclConfig.MonitoringService
	if mService == nil {
		// Replaces nil with noop monitor service (not emitting any metrics).
		mService = metrics.NoopMonitoringService{}
	}

	return &Worker{
		streamName:       kclConfig.StreamName,
		workerID:         kclConfig.WorkerID,
		processorFactory: factory,
		kclConfig:        kclConfig,
		mService:         mService,
		done:             false,
		randomSeed:       time.Now().UTC().UnixNano(),
	}
}

// WithKinesis sets the Kinesis client the worker reads through (required).
// Returns the Worker for method chaining.
func (w *Worker) WithKinesis(svc *kinesis.Client) *Worker {
	w.kc = svc
	return w
}

// WithCheckpointer sets the checkpointer that stores leases and checkpoints
// (required), e.g. from checkpoint/redis or checkpoint/dynamodb.
// Returns the Worker for method chaining.
func (w *Worker) WithCheckpointer(checker chk.Checkpointer) *Worker {
	w.checkpointer = checker
	return w
}

// Start initializes the worker and begins consuming data from the stream.
func (w *Worker) Start() error {
	log := w.kclConfig.Logger
	if err := w.initialize(); err != nil {
		log.Errorf("Failed to initialize Worker: %+v", err)
		return err
	}

	// Start monitoring service
	log.Infof("Starting monitoring service.")
	if err := w.mService.Start(); err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
		return err
	}

	log.Infof("Starting worker event loop.")
	w.waitGroup.Add(1)
	go func() {
		defer w.waitGroup.Done()
		// entering event loop
		w.eventLoop()
	}()
	return nil
}

// Shutdown signals worker to shut down. Worker will try initiating shutdown of all record processors.
func (w *Worker) Shutdown() {
	log := w.kclConfig.Logger
	log.Infof("Worker shutdown in requested.")

	if w.done || w.stop == nil {
		return
	}

	close(*w.stop)
	w.done = true
	w.waitGroup.Wait()

	w.mService.Shutdown()
	log.Infof("Worker loop is complete. Exiting from worker.")
}

// initialize
func (w *Worker) initialize() error {
	log := w.kclConfig.Logger
	log.Infof("Worker initialization in progress...")

	if w.kc == nil {
		return ErrNoKinesisClient
	}
	if w.checkpointer == nil {
		return ErrNoCheckpointer
	}

	if w.kclConfig.EnableEnhancedFanOutConsumer {
		log.Debugf("Enhanced fan-out is enabled")
		w.consumerARN = w.kclConfig.EnhancedFanOutConsumerARN
		if w.consumerARN == "" {
			var err error
			w.consumerARN, err = w.fetchConsumerARNWithRetry()
			if err != nil {
				log.Errorf("Failed to fetch consumer ARN for: %s, %v", w.kclConfig.EnhancedFanOutConsumerName, err)
				return err
			}
		}
	}

	err := w.mService.Init(w.kclConfig.ApplicationName, w.streamName, w.workerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
	}

	log.Infof("Initializing Checkpointer")
	if err := w.checkpointer.Init(); err != nil {
		log.Errorf("Failed to start Checkpointer: %+v", err)
		return err
	}

	w.shardStatus = make(map[string]*par.ShardStatus)
	w.consumers = make(map[string]bool)

	stopChan := make(chan struct{})
	w.stop = &stopChan

	w.waitGroup = &sync.WaitGroup{}

	log.Infof("Initialization complete.")

	return nil
}

// newShardConsumer creates shard consumer for the specified shard
func (w *Worker) newShardConsumer(shard *par.ShardStatus) shardConsumer {
	common := commonShardConsumer{
		shard:           shard,
		kc:              w.kc,
		checkpointer:    w.checkpointer,
		recordProcessor: w.processorFactory.CreateProcessor(),
		kclConfig:       w.kclConfig,
		mService:        w.mService,
	}
	if w.kclConfig.EnableEnhancedFanOutConsumer {
		w.kclConfig.Logger.Infof("Start enhanced fan-out shard consumer for shard: %v", shard.ID)
		return &FanOutShardConsumer{
			commonShardConsumer: common,
			consumerARN:         w.consumerARN,
			consumerID:          w.workerID,
			stop:                w.stop,
		}
	}
	w.kclConfig.Logger.Infof("Start polling shard consumer for shard: %v", shard.ID)
	return &PollingShardConsumer{
		commonShardConsumer: common,
		streamName:          w.streamName,
		consumerID:          w.workerID,
		stop:                w.stop,
		mService:            w.mService,
	}
}

// eventLoop
func (w *Worker) eventLoop() {
	log := w.kclConfig.Logger

	var foundShards int
	first, acquired := true, false
	for {
		shardSyncSleep := nextLoopDelayMillis(w.kclConfig, first, acquired)
		first, acquired = false, false

		select {
		case <-*w.stop:
			log.Infof("Shutting down...")
			return
		case <-time.After(time.Duration(shardSyncSleep) * time.Millisecond):
			log.Debugf("Waited %d ms to sync shards...", shardSyncSleep)
		}

		var ok bool
		foundShards, ok = w.syncShards(foundShards, shardSyncSleep)
		if !ok {
			continue
		}

		for _, shard := range w.acquireLeases() {
			acquired = true
			w.startConsumer(shard)
		}

		if w.kclConfig.EnableLeaseStealing {
			err := w.rebalance()
			if err != nil {
				log.Warnf("Error in rebalance: %+v", err)
			}
		}
	}
}

// startConsumer reads a just-leased shard in a new goroutine. The shard counts
// as consumed from now until the consumer has exited and released its lease,
// so the next eventLoop iteration leases it again if the consumer stopped.
func (w *Worker) startConsumer(shard *par.ShardStatus) {
	w.setConsuming(shard.ID, true)
	w.waitGroup.Add(1)
	go func() {
		defer w.waitGroup.Done()
		defer w.setConsuming(shard.ID, false)
		if err := w.newShardConsumer(shard).getRecords(); err != nil {
			w.kclConfig.Logger.Errorf("Error in getRecords: %+v", err)
		}
	}()
}

func (w *Worker) setConsuming(shardID string, consuming bool) {
	w.consumersMu.Lock()
	defer w.consumersMu.Unlock()
	if consuming {
		w.consumers[shardID] = true
	} else {
		delete(w.consumers, shardID)
	}
}

func (w *Worker) isConsuming(shardID string) bool {
	w.consumersMu.Lock()
	defer w.consumersMu.Unlock()
	return w.consumers[shardID]
}

// acquireLeases leases free shards until this worker holds its fair share (see
// leaseShare) and returns the shards it acquired; the caller starts their
// consumers. Taking the whole share in one iteration, rather than one shard per
// iteration, is what lets more shards and more workers actually speed
// consumption up. Shards are visited in map order, which Go randomises, so
// workers that start together spread over the shards instead of racing for the
// same ones; GetLease is conditional, and rebalance evens out the rest.
//
// A shard this worker does not consume is leased even when its owner is this
// worker: that lease was left behind by a consumer that exited and failed to
// release it, and GetLease renews it like any lease of this worker's own.
func (w *Worker) acquireLeases() []*par.ShardStatus {
	log := w.kclConfig.Logger

	// Refresh the view of every shard this worker does not consume first, so
	// the share counts the workers that hold leases right now.
	unreadable := map[string]bool{}
	for _, shard := range w.shardStatus {
		if w.isConsuming(shard.ID) {
			continue
		}
		// checkpoint may not exist yet is not an error condition.
		if err := w.checkpointer.FetchCheckpoint(shard); err != nil && err != chk.ErrSequenceIDNotFound {
			log.Warnf("Couldn't fetch checkpoint: %+v", err)
			unreadable[shard.ID] = true
		}
	}

	held, target := w.leaseShare()
	var acquired []*par.ShardStatus
	for _, shard := range w.shardStatus {
		if held >= target {
			break
		}
		// already consumed, unreadable, or closed and fully processed
		if w.isConsuming(shard.ID) || unreadable[shard.ID] || shard.GetCheckpoint() == chk.ShardEnd {
			continue
		}

		var stealShard bool
		if w.kclConfig.EnableLeaseStealing && shard.ClaimRequest != "" {
			upcomingStealingInterval := time.Now().UTC().Add(time.Duration(w.kclConfig.LeaseStealingIntervalMillis) * time.Millisecond)
			if shard.GetLeaseTimeout().Before(upcomingStealingInterval) && !shard.IsClaimRequestExpired(w.kclConfig) {
				if shard.ClaimRequest == w.workerID {
					stealShard = true
					log.Debugf("Stealing shard: %s", shard.ID)
				} else {
					log.Debugf("Shard being stolen: %s", shard.ID)
					continue
				}
			}
		}

		if err := w.checkpointer.GetLease(shard, w.workerID); err != nil {
			// cannot get lease on the shard
			if !errors.As(err, &chk.ErrLeaseNotAcquired{}) {
				log.Errorf("Cannot get lease: %+v", err)
			}
			continue
		}

		if stealShard {
			log.Debugf("Successfully stole shard: %+v", shard.ID)
			w.shardStealInProgress = false
		}

		// log metrics on got lease
		w.mService.LeaseGained(shard.ID)
		acquired = append(acquired, shard)
		held++
	}
	return acquired
}

// leaseShare returns how many open shards this worker consumes and how many it
// should: ceil(open shards / active workers), capped at MaxLeasesForWorker. A
// worker is active while it holds an unexpired lease (an unknown timeout counts
// as unexpired, which only makes the share smaller); this worker always counts.
// A lease of this worker's own on a shard it does not consume counts as free.
func (w *Worker) leaseShare() (held, target int) {
	now := time.Now()
	workers := map[string]bool{w.workerID: true}
	open := 0
	for _, shard := range w.shardStatus {
		if shard.GetCheckpoint() == chk.ShardEnd {
			continue
		}
		open++
		switch owner := shard.GetLeaseOwner(); {
		case w.isConsuming(shard.ID):
			held++
		case owner == w.workerID:
			// left behind by a failed release; acquireLeases leases it again
		case owner != "":
			if timeout := shard.GetLeaseTimeout(); timeout.IsZero() || timeout.After(now) {
				workers[owner] = true
			}
		}
	}
	target = (open + len(workers) - 1) / len(workers)
	if target > w.kclConfig.MaxLeasesForWorker {
		target = w.kclConfig.MaxLeasesForWorker
	}
	return held, target
}

// nextLoopDelayMillis is how long eventLoop waits before its next shard sync:
// nothing before the first one, so a starting worker leases and reads at once;
// LeaseAcquisitionIntervalMillis after an iteration that took a lease, so shards
// contended or released meanwhile follow within seconds; ShardSyncIntervalMillis
// otherwise. Both get [-50%, +50%] jitter: workers that start together then do
// not call Kinesis ListShards in lockstep and hit the API limit, and on average
// the period stays the same.
func nextLoopDelayMillis(cfg *config.KinesisClientLibConfiguration, first, acquired bool) int {
	if first {
		return 0
	}
	interval := cfg.ShardSyncIntervalMillis
	if acquired && cfg.LeaseAcquisitionIntervalMillis > 0 && cfg.LeaseAcquisitionIntervalMillis < interval {
		interval = cfg.LeaseAcquisitionIntervalMillis
	}
	rnd, _ := rand.Int(rand.Reader, big.NewInt(int64(interval)))
	return interval/2 + int(rnd.Int64())
}

func (w *Worker) rebalance() error {
	log := w.kclConfig.Logger

	workers, err := w.checkpointer.ListActiveWorkers(w.shardStatus)
	if err != nil {
		log.Debugf("Error listing workers. workerID: %s. Error: %+v ", w.workerID, err)
		return err
	}

	// A free shard is picked up by the lease path in eventLoop; stealing while
	// one exists would only move load that is about to even out on its own.
	for _, shard := range w.shardStatus {
		if shard.GetCheckpoint() != chk.ShardEnd && shard.GetLeaseOwner() == "" {
			log.Debugf("Not all shards are allocated, not stealing any. workerID: %s", w.workerID)
			return nil
		}
	}

	// Only attempt to steal one shard at time, to allow for linear convergence
	if w.shardStealInProgress {
		shardInfo := make(map[string]bool)
		err := w.getShardIDs("", shardInfo)
		if err != nil {
			return err
		}
		for _, shard := range w.shardStatus {
			if shard.ClaimRequest != "" && shard.ClaimRequest == w.workerID {
				log.Debugf("Steal in progress. workerID: %s", w.workerID)
				return nil
			}
			// Our shard steal was stomped on by a Checkpoint.
			// We could deal with that, but instead just try again
			w.shardStealInProgress = false
		}
	}

	var numShards int
	for _, shards := range workers {
		numShards += len(shards)
	}

	numWorkers := len(workers)

	// 1:1 shards to workers is optimal, so we cannot possibly rebalance
	if numWorkers >= numShards {
		log.Debugf("Optimal shard allocation, not stealing any shards. workerID: %s, %v > %v. ", w.workerID, numWorkers, numShards)
		return nil
	}

	currentShards, ok := workers[w.workerID]
	var numCurrentShards int
	if !ok {
		numCurrentShards = 0
		numWorkers++
	} else {
		numCurrentShards = len(currentShards)
	}

	optimalShards := numShards / numWorkers

	// We have more than or equal optimal shards, so no rebalancing can take place
	if numCurrentShards >= optimalShards || numCurrentShards == w.kclConfig.MaxLeasesForWorker {
		log.Debugf("We have enough shards, not attempting to steal any. workerID: %s", w.workerID)
		return nil
	}

	var workerSteal string
	for worker, shards := range workers {
		if worker != w.workerID && len(shards) > optimalShards {
			workerSteal = worker
			optimalShards = len(shards)
		}
	}
	// Not all shards are allocated so fallback to default shard allocation mechanisms
	if workerSteal == "" {
		log.Infof("Not all shards are allocated, not stealing any. workerID: %s", w.workerID)
		return nil
	}

	// Steal a random shard from the worker with the most shards
	w.shardStealInProgress = true
	rnd, _ := rand.Int(rand.Reader, big.NewInt(int64(len(workers[workerSteal]))))
	randIndex := int(rnd.Int64())
	shardToSteal := workers[workerSteal][randIndex]
	log.Debugf("Stealing shard %s from %s", shardToSteal, workerSteal)

	err = w.checkpointer.ClaimShard(w.shardStatus[shardToSteal.ID], w.workerID)
	if err != nil {
		w.shardStealInProgress = false
		return err
	}
	return nil
}

func (w *Worker) syncShards(foundShards, retryDelayMillis int) (int, bool) {
	log := w.kclConfig.Logger
	err := w.syncShard()
	if err != nil {
		if retryDelayMillis > 0 {
			log.Errorf("Error syncing shards: %+v, Retrying in %d ms...", err, retryDelayMillis)
			time.Sleep(time.Duration(retryDelayMillis) * time.Millisecond)
		} else {
			log.Errorf("Error syncing shards: %+v", err)
		}
		return foundShards, false
	}

	if foundShards == 0 || foundShards != len(w.shardStatus) {
		foundShards = len(w.shardStatus)
		log.Infof("Found %d shards", foundShards)
	}
	return foundShards, true
}

// List all shards and store them into shardStatus table
// If shard has been removed, need to exclude it from cached shard status.
func (w *Worker) getShardIDs(nextToken string, shardInfo map[string]bool) error {
	log := w.kclConfig.Logger

	args := &kinesis.ListShardsInput{}

	// When you have a nextToken, you can't set the streamName
	if nextToken != "" {
		args.NextToken = aws.String(nextToken)
	} else {
		args.StreamName = aws.String(w.streamName)
	}

	listShards, err := w.kc.ListShards(context.TODO(), args)
	if err != nil {
		log.Errorf("Error in ListShards: %s Error: %+v Request: %s", w.streamName, err, args)
		return err
	}

	for _, s := range listShards.Shards {
		// record avail shardId from fresh reading from Kinesis
		shardInfo[*s.ShardId] = true

		// found new shard
		if _, ok := w.shardStatus[*s.ShardId]; !ok {
			log.Infof("Found new shard with id %s", *s.ShardId)
			w.shardStatus[*s.ShardId] = &par.ShardStatus{
				ID:                     *s.ShardId,
				ParentShardId:          aws.ToString(s.ParentShardId),
				Mux:                    &sync.RWMutex{},
				StartingSequenceNumber: aws.ToString(s.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.ToString(s.SequenceNumberRange.EndingSequenceNumber),
			}
		}
	}

	if listShards.NextToken != nil {
		err := w.getShardIDs(aws.ToString(listShards.NextToken), shardInfo)
		if err != nil {
			log.Errorf("Error in ListShards: %s Error: %+v Request: %s", w.streamName, err, args)
			return err
		}
	}

	return nil
}

// syncShard to sync the cached shard info with actual shard info from Kinesis
func (w *Worker) syncShard() error {
	log := w.kclConfig.Logger
	shardInfo := make(map[string]bool)
	err := w.getShardIDs("", shardInfo)

	if err != nil {
		return err
	}

	for _, shard := range w.shardStatus {
		// The cached shard no longer existed, remove it.
		if _, ok := shardInfo[shard.ID]; !ok {
			// remove the shard from local status cache
			delete(w.shardStatus, shard.ID)
			// remove the shard entry in dynamoDB as well
			// Note: syncShard runs periodically. we don't need to do anything in case of error here.
			if err := w.checkpointer.RemoveLeaseInfo(shard.ID); err != nil {
				log.Errorf("Failed to remove shard lease info: %s Error: %+v", shard.ID, err)
			}
		}
	}

	return nil
}
