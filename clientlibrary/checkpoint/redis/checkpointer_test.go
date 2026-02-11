package redis

import (
	"errors"
	"sync"
	"testing"
	"time"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConfig() *config.KinesisClientLibConfiguration {
	cfg := config.NewKinesisClientLibConfig("testApp", "testStream", "us-east-1", "worker-1")
	return cfg
}

func newTestCheckpointer(mockClient *mockRedisClient, mockScript *mockScripter) *RedisCheckpoint {
	kclConfig := newTestConfig()
	cp := NewRedisCheckpoint(kclConfig, RedisConfig{Address: "localhost:6379"})
	cp.client = mockClient
	cp.scripter = mockScript
	return cp
}

func newShard(id string) *par.ShardStatus {
	return &par.ShardStatus{
		ID:  id,
		Mux: &sync.RWMutex{},
	}
}

func TestNewRedisCheckpoint(t *testing.T) {
	kclConfig := newTestConfig()

	t.Run("default prefix", func(t *testing.T) {
		cp := NewRedisCheckpoint(kclConfig, RedisConfig{Address: "localhost:6379"})
		assert.Equal(t, "kcl", cp.keyPrefix)
		assert.Equal(t, kclConfig.TableName, cp.tableName)
	})

	t.Run("custom prefix", func(t *testing.T) {
		cp := NewRedisCheckpoint(kclConfig, RedisConfig{Address: "localhost:6379", KeyPrefix: "myapp"})
		assert.Equal(t, "myapp", cp.keyPrefix)
	})
}

func TestShardKey(t *testing.T) {
	cp := newTestCheckpointer(newMockRedisClient(), newMockScripter())
	assert.Equal(t, "kcl:testApp:shard:shard-001", cp.shardKey("shard-001"))
}

func TestRegistryKey(t *testing.T) {
	cp := newTestCheckpointer(newMockRedisClient(), newMockScripter())
	assert.Equal(t, "kcl:testApp:shards", cp.registryKey())
}

func TestInit_Success(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())
	err := cp.Init()
	require.NoError(t, err)
}

func TestInit_PingFailure(t *testing.T) {
	mock := newMockRedisClient()
	mock.pingErr = errors.New("connection refused")
	cp := newTestCheckpointer(mock, newMockScripter())
	err := cp.Init()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis ping failed")
}

func TestGetLease_Success(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	scripter.setResult(cp.getLeaseScript, "OK", nil)

	shard := newShard("shard-001")
	err := cp.GetLease(shard, "worker-1")
	require.NoError(t, err)
	assert.Equal(t, "worker-1", shard.GetLeaseOwner())
	assert.False(t, shard.GetLeaseTimeout().IsZero())
}

func TestGetLease_ShardClaimed(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	scripter.setResult(cp.getLeaseScript, "SHARD_CLAIMED", nil)

	shard := newShard("shard-001")
	err := cp.GetLease(shard, "worker-1")
	require.Error(t, err)
	assert.Equal(t, chk.ErrShardClaimed, err.Error())
}

func TestGetLease_LeaseNotAcquired(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	scripter.setResult(cp.getLeaseScript, "LEASE_NOT_ACQUIRED:current lease timeout not yet expired", nil)

	shard := newShard("shard-001")
	err := cp.GetLease(shard, "worker-2")
	require.Error(t, err)
	assert.True(t, errors.As(err, &chk.ErrLeaseNotAcquired{}))
}

func TestGetLease_ScriptError(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	scripter.setResult(cp.getLeaseScript, nil, errors.New("redis down"))

	shard := newShard("shard-001")
	err := cp.GetLease(shard, "worker-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getLease script error")
}

func TestCheckpointSequence_Success(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	shard := newShard("shard-001")
	shard.SetCheckpoint("seq-123")
	shard.SetLeaseOwner("worker-1")
	shard.SetLeaseTimeout(time.Now().UTC())

	err := cp.CheckpointSequence(shard)
	require.NoError(t, err)

	key := cp.shardKey("shard-001")
	assert.Equal(t, "seq-123", mock.data[key][chk.SequenceNumberKey])
	assert.Equal(t, "worker-1", mock.data[key][chk.LeaseOwnerKey])
	assert.Equal(t, "shard-001", mock.data[key][chk.LeaseKeyKey])
	assert.True(t, mock.sets[cp.registryKey()]["shard-001"])
}

func TestCheckpointSequence_WithParentShard(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	shard := newShard("shard-001")
	shard.ParentShardId = "shard-000"
	shard.SetCheckpoint("seq-456")
	shard.SetLeaseOwner("worker-1")
	shard.SetLeaseTimeout(time.Now().UTC())

	err := cp.CheckpointSequence(shard)
	require.NoError(t, err)

	key := cp.shardKey("shard-001")
	assert.Equal(t, "shard-000", mock.data[key][chk.ParentShardIdKey])
}

func TestCheckpointSequence_HSetError(t *testing.T) {
	mock := newMockRedisClient()
	mock.hsetErr = errors.New("write error")
	cp := newTestCheckpointer(mock, newMockScripter())

	shard := newShard("shard-001")
	shard.SetCheckpoint("seq-123")
	shard.SetLeaseOwner("worker-1")
	shard.SetLeaseTimeout(time.Now().UTC())

	err := cp.CheckpointSequence(shard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint sequence failed")
}

func TestFetchCheckpoint_Success(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	key := cp.shardKey("shard-001")
	mock.data[key] = map[string]string{
		chk.SequenceNumberKey: "seq-789",
		chk.LeaseOwnerKey:     "worker-2",
		chk.LeaseTimeoutKey:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	shard := newShard("shard-001")
	err := cp.FetchCheckpoint(shard)
	require.NoError(t, err)
	assert.Equal(t, "seq-789", shard.GetCheckpoint())
	assert.Equal(t, "worker-2", shard.GetLeaseOwner())
}

func TestFetchCheckpoint_NotFound(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	shard := newShard("shard-001")
	err := cp.FetchCheckpoint(shard)
	require.Error(t, err)
	assert.True(t, errors.Is(err, chk.ErrSequenceIDNotFound))
}

func TestRemoveLeaseInfo_Success(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	key := cp.shardKey("shard-001")
	mock.data[key] = map[string]string{"ShardID": "shard-001"}
	mock.sets[cp.registryKey()] = map[string]bool{"shard-001": true}

	err := cp.RemoveLeaseInfo("shard-001")
	require.NoError(t, err)
	assert.Nil(t, mock.data[key])
	assert.False(t, mock.sets[cp.registryKey()]["shard-001"])
}

func TestRemoveLeaseOwner_Success(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	scripter.setResult(cp.removeLeaseOwnerScript, "OK", nil)

	err := cp.RemoveLeaseOwner("shard-001")
	require.NoError(t, err)
}

func TestRemoveLeaseOwner_ConditionalFail(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	scripter.setResult(cp.removeLeaseOwnerScript, "CONDITIONAL_CHECK_FAILED:owner mismatch", nil)

	err := cp.RemoveLeaseOwner("shard-001")
	require.Error(t, err)
	assert.True(t, errors.As(err, &chk.ErrLeaseNotAcquired{}))
}

func TestGetLeaseOwner_Success(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	key := cp.shardKey("shard-001")
	mock.data[key] = map[string]string{chk.LeaseOwnerKey: "worker-3"}

	owner, err := cp.GetLeaseOwner("shard-001")
	require.NoError(t, err)
	assert.Equal(t, "worker-3", owner)
}

func TestGetLeaseOwner_NotFound(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	owner, err := cp.GetLeaseOwner("shard-001")
	require.Error(t, err)
	assert.True(t, errors.Is(err, chk.ErrNoLeaseOwner))
	assert.Equal(t, "", owner)
}

func TestListActiveWorkers(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())
	cp.lastLeaseSync = time.Time{} // force sync

	registryKey := cp.registryKey()
	mock.sets[registryKey] = map[string]bool{"shard-001": true, "shard-002": true}

	mock.data[cp.shardKey("shard-001")] = map[string]string{
		chk.LeaseOwnerKey:     "worker-1",
		chk.SequenceNumberKey: "seq-100",
	}
	mock.data[cp.shardKey("shard-002")] = map[string]string{
		chk.LeaseOwnerKey:     "worker-2",
		chk.SequenceNumberKey: "seq-200",
	}

	shardStatus := map[string]*par.ShardStatus{
		"shard-001": newShard("shard-001"),
		"shard-002": newShard("shard-002"),
	}

	workers, err := cp.ListActiveWorkers(shardStatus)
	require.NoError(t, err)
	assert.Len(t, workers, 2)
	assert.Len(t, workers["worker-1"], 1)
	assert.Len(t, workers["worker-2"], 1)
}

func TestListActiveWorkers_SkipsShardEnd(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())
	cp.lastLeaseSync = time.Time{}

	registryKey := cp.registryKey()
	mock.sets[registryKey] = map[string]bool{"shard-001": true}
	mock.data[cp.shardKey("shard-001")] = map[string]string{
		chk.LeaseOwnerKey:     "worker-1",
		chk.SequenceNumberKey: chk.ShardEnd,
	}

	shard := newShard("shard-001")
	shard.SetCheckpoint(chk.ShardEnd)
	shard.SetLeaseOwner("worker-1")

	shardStatus := map[string]*par.ShardStatus{"shard-001": shard}

	workers, err := cp.ListActiveWorkers(shardStatus)
	require.NoError(t, err)
	assert.Len(t, workers, 0)
}

func TestListActiveWorkers_ThrottledSync(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())
	cp.lastLeaseSync = time.Now() // recent sync — should be throttled

	shard := newShard("shard-001")
	shard.SetCheckpoint("seq-100")
	shard.SetLeaseOwner("worker-1")

	shardStatus := map[string]*par.ShardStatus{"shard-001": shard}

	workers, err := cp.ListActiveWorkers(shardStatus)
	require.NoError(t, err)
	assert.Len(t, workers, 1)
	assert.Len(t, workers["worker-1"], 1)
}

func TestClaimShard_Success(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	key := cp.shardKey("shard-001")
	mock.data[key] = map[string]string{
		chk.SequenceNumberKey: "seq-100",
		chk.LeaseOwnerKey:     "worker-1",
		chk.LeaseTimeoutKey:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	scripter.setResult(cp.claimShardScript, "OK", nil)

	shard := newShard("shard-001")
	err := cp.ClaimShard(shard, "worker-2")
	require.NoError(t, err)
}

func TestClaimShard_ConditionalFail(t *testing.T) {
	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp := newTestCheckpointer(mock, scripter)

	key := cp.shardKey("shard-001")
	mock.data[key] = map[string]string{
		chk.SequenceNumberKey: "seq-100",
		chk.LeaseOwnerKey:     "worker-1",
		chk.LeaseTimeoutKey:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	scripter.setResult(cp.claimShardScript, "CONDITIONAL_CHECK_FAILED:claim already exists", nil)

	shard := newShard("shard-001")
	err := cp.ClaimShard(shard, "worker-2")
	require.Error(t, err)
	assert.True(t, errors.As(err, &chk.ErrLeaseNotAcquired{}))
}

func TestSyncLeases(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())
	cp.lastLeaseSync = time.Time{}

	registryKey := cp.registryKey()
	mock.sets[registryKey] = map[string]bool{"shard-001": true, "shard-002": true}

	mock.data[cp.shardKey("shard-001")] = map[string]string{
		chk.LeaseOwnerKey:     "worker-A",
		chk.SequenceNumberKey: "seq-100",
	}
	mock.data[cp.shardKey("shard-002")] = map[string]string{
		chk.LeaseOwnerKey:     "worker-B",
		chk.SequenceNumberKey: "seq-200",
	}

	shardStatus := map[string]*par.ShardStatus{
		"shard-001": newShard("shard-001"),
		"shard-002": newShard("shard-002"),
	}

	err := cp.syncLeases(shardStatus)
	require.NoError(t, err)

	assert.Equal(t, "worker-A", shardStatus["shard-001"].GetLeaseOwner())
	assert.Equal(t, "seq-100", shardStatus["shard-001"].GetCheckpoint())
	assert.Equal(t, "worker-B", shardStatus["shard-002"].GetLeaseOwner())
	assert.Equal(t, "seq-200", shardStatus["shard-002"].GetCheckpoint())
}

func TestMultiTenantKeyIsolation(t *testing.T) {
	cfg1 := config.NewKinesisClientLibConfig("app1", "stream1", "us-east-1", "worker-1")
	cfg2 := config.NewKinesisClientLibConfig("app2", "stream2", "us-east-1", "worker-1")

	cp1 := NewRedisCheckpoint(cfg1, RedisConfig{Address: "localhost:6379"})
	cp2 := NewRedisCheckpoint(cfg2, RedisConfig{Address: "localhost:6379"})

	assert.NotEqual(t, cp1.shardKey("shard-001"), cp2.shardKey("shard-001"))
	assert.NotEqual(t, cp1.registryKey(), cp2.registryKey())
	assert.Contains(t, cp1.shardKey("shard-001"), "app1")
	assert.Contains(t, cp2.shardKey("shard-001"), "app2")
}

func TestCheckpointerImplementsInterface(t *testing.T) {
	kclConfig := newTestConfig()
	cp := NewRedisCheckpoint(kclConfig, RedisConfig{Address: "localhost:6379"})

	var _ chk.Checkpointer = cp
}

func TestCreateRedisClient_HostPort(t *testing.T) {
	client, err := createRedisClient(RedisConfig{
		Address:  "localhost:6379",
		Password: "secret",
		DB:       1,
		TLS:      true,
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestCreateRedisClient_RedisURL(t *testing.T) {
	client, err := createRedisClient(RedisConfig{
		Address: "redis://localhost:6379/2",
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestCreateRedisClient_RedissURL(t *testing.T) {
	client, err := createRedisClient(RedisConfig{
		Address: "rediss://localhost:6380",
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestCreateRedisClient_RedissURLWithPasswordOverride(t *testing.T) {
	client, err := createRedisClient(RedisConfig{
		Address:  "rediss://localhost:6380/0",
		Password: "override-pwd",
		DB:       3,
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestCreateRedisClient_InvalidURL(t *testing.T) {
	_, err := createRedisClient(RedisConfig{
		Address: "redis://invalid:url:with:colons",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid redis URL")
}

func TestInit_InvalidURL(t *testing.T) {
	kclConfig := newTestConfig()
	cp := NewRedisCheckpoint(kclConfig, RedisConfig{
		Address: "redis://bad:url:format",
	})
	err := cp.Init()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis client creation failed")
}

func TestWithRedisClient(t *testing.T) {
	kclConfig := newTestConfig()
	cp := NewRedisCheckpoint(kclConfig, RedisConfig{Address: "localhost:6379"})

	mock := newMockRedisClient()
	scripter := newMockScripter()
	cp.WithRedisClient(mock, scripter)

	assert.Equal(t, mock, cp.client)
	assert.Equal(t, scripter, cp.scripter)
}

func newTestConfigHelper() *config.KinesisClientLibConfiguration {
	return config.NewKinesisClientLibConfig("testApp", "testStream", "us-east-1", "worker-1")
}

func TestFetchCheckpoint_WithLeaseTimeout(t *testing.T) {
	mock := newMockRedisClient()
	cp := newTestCheckpointer(mock, newMockScripter())

	expectedTime := time.Now().UTC().Add(10 * time.Second)
	key := cp.shardKey("shard-001")
	mock.data[key] = map[string]string{
		chk.SequenceNumberKey: "seq-789",
		chk.LeaseOwnerKey:     "worker-2",
		chk.LeaseTimeoutKey:   expectedTime.Format(time.RFC3339Nano),
	}

	shard := newShard("shard-001")
	err := cp.FetchCheckpoint(shard)
	require.NoError(t, err)

	fetchedTimeout := shard.GetLeaseTimeout()
	assert.WithinDuration(t, expectedTime, fetchedTimeout, time.Millisecond)
}

func TestRemoveLeaseInfo_DelError(t *testing.T) {
	mock := newMockRedisClient()
	mock.delErr = errors.New("delete error")
	cp := newTestCheckpointer(mock, newMockScripter())

	err := cp.RemoveLeaseInfo("shard-001")
	require.Error(t, err)
	assert.Equal(t, "delete error", err.Error())
}

func TestCheckpointSequence_SAddError(t *testing.T) {
	mock := newMockRedisClient()
	mock.saddErr = errors.New("sadd error")
	cp := newTestCheckpointer(mock, newMockScripter())

	shard := newShard("shard-001")
	shard.SetCheckpoint("seq-123")
	shard.SetLeaseOwner("worker-1")
	shard.SetLeaseTimeout(time.Now().UTC())

	err := cp.CheckpointSequence(shard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard registry add failed")
}
