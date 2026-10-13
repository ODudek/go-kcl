package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	"github.com/ODudek/go-kcl/clientlibrary/metrics"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
)

// newAcquireWorker builds worker-1 over free shards ids and a store holding
// the given leases (shard id → owner, unexpired unless expired is set).
func newAcquireWorker(cfg *config.KinesisClientLibConfiguration, ids []string, owners map[string]string, expired bool) (*Worker, *fakeCheckpointer) {
	timeout := time.Now().Add(time.Minute)
	if expired {
		timeout = time.Now().Add(-time.Minute)
	}
	cp := &fakeCheckpointer{store: map[string]*storedLease{}}
	status := map[string]*par.ShardStatus{}
	for _, id := range ids {
		status[id] = &par.ShardStatus{ID: id, Mux: &sync.RWMutex{}}
		cp.store[id] = &storedLease{checkpoint: "seq-" + id}
		if owner, ok := owners[id]; ok {
			cp.store[id].owner, cp.store[id].timeout = owner, timeout
		}
	}
	if cfg == nil {
		cfg = config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1")
	}
	return &Worker{workerID: "worker-1", kclConfig: cfg, checkpointer: cp, shardStatus: status,
		mService: metrics.NoopMonitoringService{}}, cp
}

// A worker alone on the stream takes every shard in one iteration instead of
// one shard per ShardSyncIntervalMillis.
func TestAcquireLeases_SingleWorkerTakesEveryFreeShard(t *testing.T) {
	w, cp := newAcquireWorker(nil, []string{"s1", "s2", "s3", "s4"}, nil, false)

	got := w.acquireLeases()

	assert.Len(t, got, 4)
	assert.ElementsMatch(t, []string{"s1", "s2", "s3", "s4"}, cp.leased)
}

func TestAcquireLeases_TakesOnlyItsFairShare(t *testing.T) {
	w, _ := newAcquireWorker(nil, []string{"s1", "s2", "s3", "s4"},
		map[string]string{"s1": "worker-2", "s2": "worker-2"}, false)

	assert.Len(t, w.acquireLeases(), 2, "4 shards over 2 workers")
}

func TestAcquireLeases_FairShareRoundsUp(t *testing.T) {
	w, _ := newAcquireWorker(nil, []string{"s1", "s2", "s3", "s4", "s5"},
		map[string]string{"s1": "worker-2"}, false)

	assert.Len(t, w.acquireLeases(), 3, "ceil(5/2)")
}

// An owner whose lease has expired is not an active worker: its shards are
// free, and the share is computed without it.
func TestAcquireLeases_ExpiredOwnerIsNotAnActiveWorker(t *testing.T) {
	w, _ := newAcquireWorker(nil, []string{"s1", "s2", "s3", "s4"},
		map[string]string{"s1": "worker-2", "s2": "worker-2"}, true)

	assert.Len(t, w.acquireLeases(), 4)
}

func TestAcquireLeases_RespectsMaxLeasesForWorker(t *testing.T) {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").WithMaxLeasesForWorker(1)
	w, _ := newAcquireWorker(cfg, []string{"s1", "s2", "s3"}, nil, false)

	assert.Len(t, w.acquireLeases(), 1)
}

func TestAcquireLeases_OwnLeasesCountTowardsTheShare(t *testing.T) {
	w, _ := newAcquireWorker(nil, []string{"s1", "s2", "s3", "s4"},
		map[string]string{"s1": "worker-1", "s2": "worker-2"}, false)
	w.shardStatus["s1"].SetLeaseOwner("worker-1")

	assert.Len(t, w.acquireLeases(), 1, "share 2, already holding 1")
}

func TestAcquireLeases_SkipsClosedShards(t *testing.T) {
	w, cp := newAcquireWorker(nil, []string{"s1", "s2"}, nil, false)
	cp.store["s1"].checkpoint = chk.ShardEnd

	got := w.acquireLeases()

	require.Len(t, got, 1)
	assert.Equal(t, "s2", got[0].ID)
}

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

// storedLease is one shard's row in fakeCheckpointer's store.
type storedLease struct {
	owner      string
	timeout    time.Time
	checkpoint string
}

// fakeCheckpointer answers ListActiveWorkers from the shard map it is given and
// records ClaimShard calls. With a store, FetchCheckpoint reads it and GetLease
// is conditional like the real checkpointers: a lease held by another worker
// until a future timeout is not acquired.
type fakeCheckpointer struct {
	claims []string
	store  map[string]*storedLease
	leased []string
}

func (f *fakeCheckpointer) Init() error                               { return nil }
func (f *fakeCheckpointer) CheckpointSequence(*par.ShardStatus) error { return nil }
func (f *fakeCheckpointer) FetchCheckpoint(s *par.ShardStatus) error {
	row, ok := f.store[s.ID]
	if !ok {
		return chk.ErrSequenceIDNotFound
	}
	s.SetLeaseOwner(row.owner)
	s.SetLeaseTimeout(row.timeout)
	if row.checkpoint == "" {
		return chk.ErrSequenceIDNotFound
	}
	s.SetCheckpoint(row.checkpoint)
	return nil
}
func (f *fakeCheckpointer) GetLease(s *par.ShardStatus, worker string) error {
	if f.store == nil {
		return nil
	}
	row, ok := f.store[s.ID]
	if !ok {
		row = &storedLease{}
		f.store[s.ID] = row
	}
	if row.owner != "" && row.owner != worker && row.timeout.After(time.Now()) {
		return chk.ErrLeaseNotAcquired{Cause: "held by " + row.owner}
	}
	row.owner, row.timeout = worker, time.Now().Add(time.Minute)
	s.SetLeaseOwner(worker)
	s.SetLeaseTimeout(row.timeout)
	f.leased = append(f.leased, s.ID)
	return nil
}
func (f *fakeCheckpointer) RemoveLeaseInfo(string) error         { return nil }
func (f *fakeCheckpointer) RemoveLeaseOwner(string) error        { return nil }
func (f *fakeCheckpointer) GetLeaseOwner(string) (string, error) { return "", nil }
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
