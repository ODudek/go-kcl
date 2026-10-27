package test

import (
	"log/slog"
	"testing"

	cfg "github.com/ODudek/go-kcl/clientlibrary/config"
	wk "github.com/ODudek/go-kcl/clientlibrary/worker"
	"github.com/ODudek/go-kcl/logger"
)

func TestLeaseStealing(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName,
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}
	test := NewLeaseStealingTest(t, config, newLeaseStealingWorkerFactory(t))
	test.Run(LeaseStealingAssertions{
		expectedLeasesForInitialWorker: config.numShards,
		expectedLeasesPerWorker:        config.numShards / config.numWorkers,
	})
}

type leaseStealingWorkerFactory struct {
	t *testing.T
}

func newLeaseStealingWorkerFactory(t *testing.T) *leaseStealingWorkerFactory {
	return &leaseStealingWorkerFactory{t}
}

func (wf *leaseStealingWorkerFactory) CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration {
	log := newTestLogger(slog.LevelError).WithFields(logger.Fields{"worker": workerID})

	return cfg.NewKinesisClientLibConfig(config.appName, config.streamName, config.regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(10000).
		WithLeaseStealing(true).
		WithLogger(log)
}

func (wf *leaseStealingWorkerFactory) CreateWorker(_ string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	return newTestWorker(wf.t, recordProcessorFactory(wf.t), kclConfig)
}

func TestLeaseStealingInjectCheckpointer(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName,
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}
	test := NewLeaseStealingTest(t, config, newleaseStealingWorkerFactoryCustomChk(t))
	test.Run(LeaseStealingAssertions{
		expectedLeasesForInitialWorker: config.numShards,
		expectedLeasesPerWorker:        config.numShards / config.numWorkers,
	})
}

type leaseStealingWorkerFactoryCustom struct {
	*leaseStealingWorkerFactory
}

func newleaseStealingWorkerFactoryCustomChk(t *testing.T) *leaseStealingWorkerFactoryCustom {
	return &leaseStealingWorkerFactoryCustom{
		newLeaseStealingWorkerFactory(t),
	}
}

func (wfc *leaseStealingWorkerFactoryCustom) CreateWorker(workerID string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	worker := wfc.leaseStealingWorkerFactory.CreateWorker(workerID, kclConfig)
	return worker.WithCheckpointer(newDynamoCheckpoint(wfc.t, kclConfig))
}

func TestLeaseStealingWithMaxLeasesForWorker(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName,
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}
	test := NewLeaseStealingTest(t, config, newLeaseStealingWorkerFactoryMaxLeases(t, config.numShards-1))
	test.Run(LeaseStealingAssertions{
		expectedLeasesForInitialWorker: config.numShards - 1,
		expectedLeasesPerWorker:        2,
	})
}

type leaseStealingWorkerFactoryMaxLeases struct {
	maxLeases int
	*leaseStealingWorkerFactory
}

func newLeaseStealingWorkerFactoryMaxLeases(t *testing.T, maxLeases int) *leaseStealingWorkerFactoryMaxLeases {
	return &leaseStealingWorkerFactoryMaxLeases{
		maxLeases,
		newLeaseStealingWorkerFactory(t),
	}
}

func (wfm *leaseStealingWorkerFactoryMaxLeases) CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration {
	kclConfig := wfm.leaseStealingWorkerFactory.CreateKCLConfig(workerID, config)
	kclConfig.WithMaxLeasesForWorker(wfm.maxLeases)
	return kclConfig
}
