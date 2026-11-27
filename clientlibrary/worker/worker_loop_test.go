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
	w.consumers = map[string]bool{"s1": true}

	got := w.acquireLeases()

	require.Len(t, got, 1, "share 2, already consuming 1")
	assert.NotEqual(t, "s1", got[0].ID)
}

// Issue #11: a shard consumer exited and failed to release its lease, so the
// store still names this worker as the owner, and reading it back puts this
// worker in memory as the owner again. No consumer reads those shards; they
// must be leased again instead of being skipped as already owned.
func TestAcquireLeases_RetakesOwnLeasesWithoutAConsumer(t *testing.T) {
	w, cp := newAcquireWorker(nil, []string{"s1", "s2", "s3"},
		map[string]string{"s1": "worker-1", "s2": "worker-1", "s3": "worker-1"}, false)
	w.shardStatus["s1"].SetLeaseOwner("worker-1")
	w.consumers = map[string]bool{"s3": true}

	got := w.acquireLeases()

	assert.Len(t, got, 2)
	assert.ElementsMatch(t, []string{"s1", "s2"}, cp.leased)
}

// A shard is leased again only once its consumer has exited, whatever its
// owner in memory says.
func TestAcquireLeases_SkipsShardsBeingConsumed(t *testing.T) {
	w, _ := newAcquireWorker(nil, []string{"s1"}, nil, false)
	w.consumers = map[string]bool{}

	w.setConsuming("s1", true)
	assert.Empty(t, w.acquireLeases())

	w.setConsuming("s1", false)
	assert.Len(t, w.acquireLeases(), 1)
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
	claim      string
}

// fakeCheckpointer answers ListActiveWorkers from the shard map it is given and
// records ClaimShard calls. With a store, FetchCheckpoint reads it and GetLease
// is conditional like the real checkpointers: a lease held by another worker
// until a future timeout is not acquired.
type fakeCheckpointer struct {
	claims []string
	store  map[string]*storedLease
	leased []string
	// lostClaims makes ClaimShard fail for these shards, as a checkpointer
	// does when the lease changed since it was read.
	lostClaims map[string]bool
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
	s.SetClaimRequest(row.claim)
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
	if f.lostClaims[s.ID] {
		return chk.ErrLeaseNotAcquired{Cause: "lease changed"}
	}
	f.claims = append(f.claims, s.ID)
	s.SetClaimRequest(w)
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
	return &Worker{workerID: "worker-1", kclConfig: cfg, checkpointer: cp, shardStatus: status, consumers: map[string]bool{}}
}

// ownedShards returns one shard owned by owner per id.
func ownedShards(owner string, ids ...string) []*par.ShardStatus {
	shards := make([]*par.ShardStatus, 0, len(ids))
	for _, id := range ids {
		shards = append(shards, ownedShard(id, owner))
	}
	return shards
}

// claimedFrom counts the claims placed on the shards of each owner.
func claimedFrom(w *Worker, cp *fakeCheckpointer) map[string]int {
	byOwner := map[string]int{}
	for _, id := range cp.claims {
		byOwner[w.shardStatus[id].GetLeaseOwner()]++
	}
	return byOwner
}

func shardsOf(groups ...[]*par.ShardStatus) []*par.ShardStatus {
	var all []*par.ShardStatus
	for _, g := range groups {
		all = append(all, g...)
	}
	return all
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
}

func TestRebalance_StealsFromTheBusiestWorkerWhenAllShardsAreAllocated(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp,
		ownedShard("s1", "worker-2"), ownedShard("s2", "worker-2"),
		ownedShard("s3", "worker-2"), ownedShard("s4", "worker-2"))

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 2, "4 shards over 2 workers: the whole share in one pass")
}

func TestRebalance_ClosedShardsDoNotCountAsUnallocated(t *testing.T) {
	cp := &fakeCheckpointer{}
	closed := ownedShard("s5", "")
	closed.SetCheckpoint(chk.ShardEnd)
	w := newRebalanceWorker(cp,
		ownedShard("s1", "worker-2"), ownedShard("s2", "worker-2"),
		ownedShard("s3", "worker-2"), ownedShard("s4", "worker-2"), closed)

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 2)
}

// releasedClaimedShard is a store holding s1, just released by its owner (no
// owner, lease timeout now) and claimed by claimer, so the claim is live.
func releasedClaimedShard(claimer string) map[string]*storedLease {
	return map[string]*storedLease{"s1": {timeout: time.Now(), checkpoint: "seq-s1", claim: claimer}}
}

// With lease stealing on, a shard another worker has claimed is left to that
// worker during the claim window, even once its owner has released it.
func TestAcquireLeases_LeavesAShardClaimedByAnotherWorkerToTheClaimer(t *testing.T) {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").WithLeaseStealing(true)
	w, cp := newAcquireWorker(cfg, []string{"s1"}, nil, false)
	cp.store = releasedClaimedShard("worker-3")

	assert.Empty(t, w.acquireLeases())
	assert.Empty(t, cp.leased)
}

func TestAcquireLeases_ClaimerTakesTheShardItClaimed(t *testing.T) {
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").WithLeaseStealing(true)
	w, cp := newAcquireWorker(cfg, []string{"s1"}, nil, false)
	cp.store = releasedClaimedShard("worker-1")

	assert.Len(t, w.acquireLeases(), 1)
	assert.Equal(t, []string{"s1"}, cp.leased)
}

// A joining worker claims its whole share in one pass, from the workers that are
// furthest above it, and leaves nobody below it: 10 shards over 4 workers end at
// 2/2/3/3 once the claimed shards move.
func TestRebalance_ClaimsTheWholeShareInOnePassFromTheBusiestWorkers(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp, shardsOf(
		ownedShards("worker-2", "s1", "s2", "s3", "s4"),
		ownedShards("worker-3", "s5", "s6", "s7"),
		ownedShards("worker-4", "s8", "s9", "s10"))...)

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 2, "share = floor(10 / 4) = 2")
	assert.Equal(t, map[string]int{"worker-2": 2}, claimedFrom(w, cp), "worker-2 goes from 4 to 2; workers 3 and 4 keep 3")
}

func TestRebalance_NeverTakesAWorkerBelowItsShare(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp, shardsOf(
		ownedShards("worker-2", "s1", "s2", "s3", "s4", "s5"),
		ownedShards("worker-3", "s6", "s7"))...)

	require.NoError(t, w.rebalance())
	assert.Equal(t, map[string]int{"worker-2": 2}, claimedFrom(w, cp), "share = floor(7 / 3) = 2; worker-3 is at it")
}

// With the rounded-down share, a worker at the share doesn't claim: with the
// rounded-up one, workers at 2 would keep taking shards from workers at 3.
func TestRebalance_DoesNotOscillateWhenTheShardsDontDivideEvenly(t *testing.T) {
	cp := &fakeCheckpointer{}
	mine := ownedShards("worker-1", "s1", "s2")
	w := newRebalanceWorker(cp, shardsOf(mine,
		ownedShards("worker-2", "s3", "s4", "s5"),
		ownedShards("worker-3", "s6", "s7", "s8"),
		ownedShards("worker-4", "s9", "s10"))...)
	for _, s := range mine {
		w.setConsuming(s.ID, true)
	}

	require.NoError(t, w.rebalance())
	assert.Empty(t, cp.claims, "2/3/3/2 is balanced")
}

func TestRebalance_CountsLivePendingClaims(t *testing.T) {
	cp := &fakeCheckpointer{}
	shards := ownedShards("worker-2", "s1", "s2", "s3", "s4")
	shards[0].SetClaimRequest("worker-1")
	w := newRebalanceWorker(cp, shards...)

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 1, "share 2, one claim already pending")
	assert.NotContains(t, cp.claims, "s1")
	assert.Equal(t, 2, w.livePendingClaims())
}

// An expired claim no longer reserves the shard, so it must not count, or it
// would block the next claim.
func TestRebalance_ExpiredClaimDoesNotCount(t *testing.T) {
	cp := &fakeCheckpointer{}
	shards := ownedShards("worker-2", "s1", "s2", "s3", "s4")
	for _, s := range shards {
		s.SetLeaseTimeout(time.Now().Add(time.Minute))
	}
	shards[0].SetClaimRequest("worker-1")
	shards[0].SetLeaseTimeout(time.Now().Add(-time.Hour)) // expired lease, so the claim has expired too
	w := newRebalanceWorker(cp, shards...)
	require.Equal(t, 0, w.livePendingClaims())

	require.NoError(t, w.rebalance())
	assert.Empty(t, cp.claims, "s1 is free (expired lease): the lease path takes it, no stealing")
}

func TestRebalance_MaxLeasesToStealAtOneTimeCapsClaimsInFlight(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp, ownedShards("worker-2", "s1", "s2", "s3", "s4", "s5", "s6")...)
	w.kclConfig.MaxLeasesToStealAtOneTime = 2

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 2, "share 3, but at most 2 in flight")

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 2, "both still pending: no new claim")
}

func TestRebalance_RespectsMaxLeasesForWorker(t *testing.T) {
	cp := &fakeCheckpointer{}
	w := newRebalanceWorker(cp, ownedShards("worker-2", "s1", "s2", "s3", "s4")...)
	w.kclConfig.MaxLeasesForWorker = 1

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 1)
}

// Shards another worker has claimed are on their way to it: they are neither
// claimed again nor counted as the victim's.
func TestRebalance_SkipsShardsClaimedByAnotherWorker(t *testing.T) {
	cp := &fakeCheckpointer{}
	shards := ownedShards("worker-2", "s1", "s2", "s3", "s4", "s5", "s6")
	shards[0].SetClaimRequest("worker-3")
	w := newRebalanceWorker(cp, shards...)

	require.NoError(t, w.rebalance())
	assert.NotContains(t, cp.claims, "s1")
	assert.Len(t, cp.claims, 2, "share 3 of 6; worker-2 keeps at least 3 of the 5 not claimed yet")
}

// A claim lost to a concurrent change (checkpoint, renewal, other claim) is
// skipped and another shard is claimed instead, up to the deficit.
func TestRebalance_TriesAnotherShardWhenAClaimIsLost(t *testing.T) {
	cp := &fakeCheckpointer{lostClaims: map[string]bool{"s1": true, "s2": true}}
	w := newRebalanceWorker(cp, ownedShards("worker-2", "s1", "s2", "s3", "s4", "s5", "s6")...)

	require.NoError(t, w.rebalance())
	assert.Len(t, cp.claims, 3)
	assert.NotContains(t, cp.claims, "s1")
	assert.NotContains(t, cp.claims, "s2")
}

func TestRebalance_ExpiredLeaseCountsAsFreeNotAsAWorker(t *testing.T) {
	cp := &fakeCheckpointer{}
	shards := ownedShards("worker-2", "s1", "s2", "s3", "s4")
	shards[3].SetLeaseOwner("worker-gone")
	shards[3].SetLeaseTimeout(time.Now().Add(-time.Minute))
	w := newRebalanceWorker(cp, shards...)

	require.NoError(t, w.rebalance())
	assert.Empty(t, cp.claims, "s4 is free: acquireLeases takes it first")
}

// While a lease-stealing claim is pending, eventLoop comes back after
// LeaseAcquisitionIntervalMillis, so the claimed shard is taken soon after its
// owner releases it instead of after a full ShardSyncIntervalMillis.
func TestWantsQuickLoop(t *testing.T) {
	claimed := ownedShard("s1", "worker-2")
	claimed.SetLeaseTimeout(time.Now().Add(time.Minute))
	claimed.SetClaimRequest("worker-1")
	w := newRebalanceWorker(&fakeCheckpointer{}, claimed, ownedShard("s2", "worker-2"))

	assert.True(t, w.wantsQuickLoop(false), "a live claim is pending")
	assert.True(t, w.wantsQuickLoop(true), "took a lease")

	claimed.SetClaimRequest("")
	assert.False(t, w.wantsQuickLoop(false), "nothing taken, nothing pending")

	claimed.SetClaimRequest("worker-1")
	claimed.SetLeaseTimeout(time.Now().Add(-time.Hour))
	assert.False(t, w.wantsQuickLoop(false), "an expired claim isn't pending")

	claimed.SetLeaseTimeout(time.Now().Add(time.Minute))
	w.kclConfig.EnableLeaseStealing = false
	assert.False(t, w.wantsQuickLoop(false), "without lease stealing, claims are ignored")
}
