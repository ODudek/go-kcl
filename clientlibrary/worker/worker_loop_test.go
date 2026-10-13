package worker

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
)

func TestNextLoopDelayMillis_FirstIterationIsImmediate(t *testing.T) {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1")

	assert.Equal(t, 0, nextLoopDelayMillis(cfg, true, false))
	assert.Equal(t, 0, nextLoopDelayMillis(cfg, true, true))
}

func TestNextLoopDelayMillis_AfterAcquiringALeaseUsesTheAcquisitionInterval(t *testing.T) {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").
		WithShardSyncIntervalMillis(60000).
		WithLeaseAcquisitionIntervalMillis(2000)

	for i := 0; i < 200; i++ {
		d := nextLoopDelayMillis(cfg, false, true)
		require.GreaterOrEqual(t, d, 1000)
		require.Less(t, d, 3000)
	}
}

func TestNextLoopDelayMillis_IdleIterationUsesTheShardSyncInterval(t *testing.T) {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").
		WithShardSyncIntervalMillis(60000).
		WithLeaseAcquisitionIntervalMillis(2000)

	for i := 0; i < 200; i++ {
		d := nextLoopDelayMillis(cfg, false, false)
		require.GreaterOrEqual(t, d, 30000)
		require.Less(t, d, 90000)
	}
}

// fakeCheckpointer answers ListActiveWorkers from the shard map it is given and
// records ClaimShard calls; every other method is unused by rebalance.
type fakeCheckpointer struct {
	claims []string
}

func (f *fakeCheckpointer) Init() error                               { return nil }
func (f *fakeCheckpointer) GetLease(*par.ShardStatus, string) error   { return nil }
func (f *fakeCheckpointer) CheckpointSequence(*par.ShardStatus) error { return nil }
func (f *fakeCheckpointer) FetchCheckpoint(*par.ShardStatus) error    { return nil }
func (f *fakeCheckpointer) RemoveLeaseInfo(string) error              { return nil }
func (f *fakeCheckpointer) RemoveLeaseOwner(string) error             { return nil }
func (f *fakeCheckpointer) GetLeaseOwner(string) (string, error)      { return "", nil }
func (f *fakeCheckpointer) ClaimShard(s *par.ShardStatus, w string) error {
	f.claims = append(f.claims, s.ID)
	return nil
}
func (f *fakeCheckpointer) ListActiveWorkers(shards map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	workers := map[string][]*par.ShardStatus{}
	for _, s := range shards {
		if s.GetCheckpoint() == chk.ShardEnd || s.GetLeaseOwner() == "" {
			continue
		}
		workers[s.GetLeaseOwner()] = append(workers[s.GetLeaseOwner()], s)
	}
	return workers, nil
}

func ownedShard(id, owner string) *par.ShardStatus {
	s := &par.ShardStatus{ID: id, Mux: &sync.RWMutex{}}
	s.SetCheckpoint("seq-" + id)
	s.SetLeaseOwner(owner)
	return s
}

func newRebalanceWorker(cp chk.Checkpointer, shards ...*par.ShardStatus) *Worker {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").
		WithLeaseStealing(true)
	status := map[string]*par.ShardStatus{}
	for _, s := range shards {
		status[s.ID] = s
	}
	return &Worker{workerID: "worker-1", kclConfig: cfg, checkpointer: cp, shardStatus: status}
}

// A free shard is picked up by the normal lease path; until it is, rebalance
// must neither steal nor report an error (it used to fail with
// AssignedToNotFoundForShard on every loop).
func TestRebalance_UnallocatedShardSkipsStealingWithoutError(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp,
		ownedShard("s1", "worker-2"), ownedShard("s2", "worker-2"),
		ownedShard("s3", "worker-2"), ownedShard("s4", ""))

	require.NoError(t, w.rebalance())
	assert.Empty(t, cp.claims)
	assert.False(t, w.shardStealInProgress)
}

func TestRebalance_StealsFromTheBusiestWorkerWhenAllShardsAreAllocated(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp,
		ownedShard("s1", "worker-2"), ownedShard("s2", "worker-2"),
		ownedShard("s3", "worker-2"), ownedShard("s4", "worker-2"))

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 1)
	assert.True(t, w.shardStealInProgress)
}

func TestRebalance_ClosedShardsDoNotCountAsUnallocated(t *testing.T) {
	cp := &fakeCheckpointer{}
	closed := ownedShard("s5", "")
	closed.SetCheckpoint(chk.ShardEnd)
	w := newRebalanceWorker(cp,
		ownedShard("s1", "worker-2"), ownedShard("s2", "worker-2"),
		ownedShard("s3", "worker-2"), ownedShard("s4", "worker-2"), closed)

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 1)
}
