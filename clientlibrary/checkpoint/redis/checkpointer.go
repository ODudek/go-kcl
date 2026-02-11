package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/ODudek/go-kcl/logger"
)

const defaultKeyPrefix = "kcl"

// RedisClient is the minimal interface over *redis.Client used by the checkpointer.
// *redis.Client satisfies this naturally.
type RedisClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	Close() error
}

// Scripter is the interface for running Lua scripts (satisfied by *redis.Client).
type Scripter interface {
	redis.Scripter
}

// RedisConfig holds connection settings for the Redis backend.
type RedisConfig struct {
	Address   string // host:port (required)
	Password  string // auth password (optional)
	DB        int    // database number 0-15 (default: 0)
	KeyPrefix string // key prefix (default: "kcl")
	TLS       bool   // enable TLS (default: false)
}

// RedisCheckpoint implements the checkpoint.Checkpointer interface using Redis.
type RedisCheckpoint struct {
	log       logger.Logger
	client    RedisClient
	scripter  Scripter
	kclConfig *config.KinesisClientLibConfiguration
	redisCfg  RedisConfig

	tableName string
	keyPrefix string

	leaseDuration int
	lastLeaseSync time.Time

	getLeaseScript         *redis.Script
	claimShardScript       *redis.Script
	removeLeaseOwnerScript *redis.Script
}

// NewRedisCheckpoint creates a new Redis-backed checkpointer.
func NewRedisCheckpoint(kclConfig *config.KinesisClientLibConfiguration, redisCfg RedisConfig) *RedisCheckpoint {
	prefix := redisCfg.KeyPrefix
	if prefix == "" {
		prefix = defaultKeyPrefix
	}

	return &RedisCheckpoint{
		log:           kclConfig.Logger,
		kclConfig:     kclConfig,
		redisCfg:      redisCfg,
		tableName:     kclConfig.TableName,
		keyPrefix:     prefix,
		leaseDuration: kclConfig.FailoverTimeMillis,

		getLeaseScript:         redis.NewScript(scriptGetLeaseSrc),
		claimShardScript:       redis.NewScript(scriptClaimShardSrc),
		removeLeaseOwnerScript: redis.NewScript(scriptRemoveLeaseOwnerSrc),
	}
}

// WithRedisClient injects a pre-configured Redis client (useful for testing).
func (c *RedisCheckpoint) WithRedisClient(client RedisClient, scripter Scripter) *RedisCheckpoint {
	c.client = client
	c.scripter = scripter
	return c
}

// shardKey returns the Redis hash key for a shard.
func (c *RedisCheckpoint) shardKey(shardID string) string {
	return fmt.Sprintf("%s:%s:shard:%s", c.keyPrefix, c.tableName, shardID)
}

// registryKey returns the Redis set key tracking all shard IDs.
func (c *RedisCheckpoint) registryKey() string {
	return fmt.Sprintf("%s:%s:shards", c.keyPrefix, c.tableName)
}

// Init initialises the Redis connection and verifies connectivity.
func (c *RedisCheckpoint) Init() error {
	c.log.Infof("Creating Redis session for table %s", c.tableName)

	if c.client == nil {
		client, err := createRedisClient(c.redisCfg)
		if err != nil {
			return fmt.Errorf("redis client creation failed: %w", err)
		}
		c.client = client
		c.scripter = client
	}

	if err := c.client.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}

	return nil
}

// createRedisClient builds a *redis.Client from RedisConfig.
// If Address looks like a URL (redis:// or rediss://), it is parsed automatically.
// The rediss:// scheme enables TLS. The explicit TLS field acts as an override
// on top of a plain host:port address.
func createRedisClient(cfg RedisConfig) (*redis.Client, error) {
	if strings.HasPrefix(cfg.Address, "redis://") || strings.HasPrefix(cfg.Address, "rediss://") {
		opts, err := redis.ParseURL(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("invalid redis URL %q: %w", cfg.Address, err)
		}
		if cfg.Password != "" {
			opts.Password = cfg.Password
		}
		if cfg.DB != 0 {
			opts.DB = cfg.DB
		}
		if cfg.TLS && opts.TLSConfig == nil {
			opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return redis.NewClient(opts), nil
	}

	opts := &redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return redis.NewClient(opts), nil
}

// GetLease attempts to gain a lock on the given shard.
func (c *RedisCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(c.leaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	nowUTC := time.Now().UTC().Format(time.RFC3339Nano)

	enableStealing := "0"
	if c.kclConfig.EnableLeaseStealing {
		enableStealing = "1"
	}

	isClaimExpired := "0"
	if shard.IsClaimRequestExpired(c.kclConfig) {
		isClaimExpired = "1"
	}

	checkpoint := shard.GetCheckpoint()
	parentShardId := shard.ParentShardId

	keys := []string{c.shardKey(shard.ID), c.registryKey()}
	args := []interface{}{
		newAssignTo,
		newLeaseTimeoutString,
		shard.ID,
		nowUTC,
		enableStealing,
		isClaimExpired,
		checkpoint,
		parentShardId,
	}

	result, err := c.getLeaseScript.Run(context.Background(), c.scripter, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("getLease script error: %w", err)
	}

	resultStr, ok := result.(string)
	if !ok {
		return fmt.Errorf("unexpected getLease result type: %T", result)
	}

	switch {
	case resultStr == "OK":
		shard.Mux.Lock()
		shard.AssignedTo = newAssignTo
		shard.LeaseTimeout = newLeaseTimeout
		shard.Mux.Unlock()
		return nil
	case resultStr == "SHARD_CLAIMED":
		return errors.New(chk.ErrShardClaimed)
	case strings.HasPrefix(resultStr, "LEASE_NOT_ACQUIRED:"):
		reason := strings.TrimPrefix(resultStr, "LEASE_NOT_ACQUIRED:")
		return chk.ErrLeaseNotAcquired{Cause: reason}
	default:
		return fmt.Errorf("unexpected getLease result: %s", resultStr)
	}
}

// CheckpointSequence writes a checkpoint at the designated sequence ID.
func (c *RedisCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {
	leaseTimeout := shard.GetLeaseTimeout().UTC().Format(time.RFC3339Nano)

	fields := []interface{}{
		chk.LeaseKeyKey, shard.ID,
		chk.SequenceNumberKey, shard.GetCheckpoint(),
		chk.LeaseOwnerKey, shard.GetLeaseOwner(),
		chk.LeaseTimeoutKey, leaseTimeout,
	}

	if shard.ParentShardId != "" {
		fields = append(fields, chk.ParentShardIdKey, shard.ParentShardId)
	}

	if err := c.client.HSet(context.Background(), c.shardKey(shard.ID), fields...).Err(); err != nil {
		return fmt.Errorf("checkpoint sequence failed: %w", err)
	}

	if err := c.client.SAdd(context.Background(), c.registryKey(), shard.ID).Err(); err != nil {
		return fmt.Errorf("shard registry add failed: %w", err)
	}

	return nil
}

// FetchCheckpoint retrieves the checkpoint for the given shard.
func (c *RedisCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {
	data, err := c.client.HGetAll(context.Background(), c.shardKey(shard.ID)).Result()
	if err != nil {
		return fmt.Errorf("fetch checkpoint failed: %w", err)
	}

	sequenceID, ok := data[chk.SequenceNumberKey]
	if !ok || sequenceID == "" {
		return chk.ErrSequenceIDNotFound
	}

	c.log.Debugf("Retrieved Shard Iterator %s", sequenceID)
	shard.SetCheckpoint(sequenceID)

	if assignedTo, ok := data[chk.LeaseOwnerKey]; ok && assignedTo != "" {
		shard.SetLeaseOwner(assignedTo)
	}

	if leaseTimeout, ok := data[chk.LeaseTimeoutKey]; ok && leaseTimeout != "" {
		t, err := time.Parse(time.RFC3339Nano, leaseTimeout)
		if err != nil {
			return fmt.Errorf("parse lease timeout failed: %w", err)
		}
		shard.SetLeaseTimeout(t)
	}

	return nil
}

// RemoveLeaseInfo removes all lease info for a shard (shard no longer exists).
func (c *RedisCheckpoint) RemoveLeaseInfo(shardID string) error {
	if err := c.client.Del(context.Background(), c.shardKey(shardID)).Err(); err != nil {
		c.log.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
		return err
	}

	if err := c.client.SRem(context.Background(), c.registryKey(), shardID).Err(); err != nil {
		c.log.Errorf("Error removing shard from registry: %s, Error: %+v", shardID, err)
		return err
	}

	c.log.Infof("Lease info for shard: %s has been removed.", shardID)
	return nil
}

// RemoveLeaseOwner conditionally removes the lease owner if it matches this worker.
func (c *RedisCheckpoint) RemoveLeaseOwner(shardID string) error {
	keys := []string{c.shardKey(shardID)}
	args := []interface{}{c.kclConfig.WorkerID}

	result, err := c.removeLeaseOwnerScript.Run(context.Background(), c.scripter, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("removeLeaseOwner script error: %w", err)
	}

	resultStr, ok := result.(string)
	if !ok {
		return fmt.Errorf("unexpected removeLeaseOwner result type: %T", result)
	}

	if strings.HasPrefix(resultStr, "CONDITIONAL_CHECK_FAILED:") {
		reason := strings.TrimPrefix(resultStr, "CONDITIONAL_CHECK_FAILED:")
		return chk.ErrLeaseNotAcquired{Cause: reason}
	}

	return nil
}

// GetLeaseOwner returns the current lease owner for a shard.
func (c *RedisCheckpoint) GetLeaseOwner(shardID string) (string, error) {
	data, err := c.client.HGetAll(context.Background(), c.shardKey(shardID)).Result()
	if err != nil {
		return "", fmt.Errorf("get lease owner failed: %w", err)
	}

	assignedTo, ok := data[chk.LeaseOwnerKey]
	if !ok || assignedTo == "" {
		return "", chk.ErrNoLeaseOwner
	}

	return assignedTo, nil
}

// ListActiveWorkers returns a map of workers to their assigned shards.
func (c *RedisCheckpoint) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	if err := c.syncLeases(shardStatus); err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == chk.ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			c.log.Debugf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, c.kclConfig.WorkerID)
			return nil, chk.ErrShardNotAssigned
		}

		workers[leaseOwner] = append(workers[leaseOwner], shard)
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt.
func (c *RedisCheckpoint) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := c.FetchCheckpoint(shard)
	if err != nil && !errors.Is(err, chk.ErrSequenceIDNotFound) {
		return err
	}

	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339Nano)
	expectedOwner := shard.GetLeaseOwner()
	expectedCheckpoint := shard.GetCheckpoint()

	keys := []string{c.shardKey(shard.ID)}
	args := []interface{}{
		shard.ID,
		claimID,
		leaseTimeoutString,
		expectedOwner,
		expectedCheckpoint,
		shard.ParentShardId,
	}

	result, err := c.claimShardScript.Run(context.Background(), c.scripter, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("claimShard script error: %w", err)
	}

	resultStr, ok := result.(string)
	if !ok {
		return fmt.Errorf("unexpected claimShard result type: %T", result)
	}

	if strings.HasPrefix(resultStr, "CONDITIONAL_CHECK_FAILED:") {
		reason := strings.TrimPrefix(resultStr, "CONDITIONAL_CHECK_FAILED:")
		return chk.ErrLeaseNotAcquired{Cause: reason}
	}

	return nil
}

// syncLeases fetches all shard data from Redis and updates the in-memory ShardStatus map.
func (c *RedisCheckpoint) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	syncInterval := time.Duration(c.kclConfig.LeaseSyncingTimeIntervalMillis) * time.Millisecond
	if c.lastLeaseSync.Add(syncInterval).After(time.Now()) {
		return nil
	}

	c.lastLeaseSync = time.Now()

	shardIDs, err := c.client.SMembers(context.Background(), c.registryKey()).Result()
	if err != nil {
		c.log.Debugf("Error fetching shard registry: %+v", err)
		return err
	}

	if len(shardIDs) == 0 {
		return nil
	}

	for _, sid := range shardIDs {
		data, err := c.client.HGetAll(context.Background(), c.shardKey(sid)).Result()
		if err != nil {
			continue
		}

		shard, ok := shardStatus[sid]
		if !ok {
			continue
		}

		if assignedTo, found := data[chk.LeaseOwnerKey]; found && assignedTo != "" {
			shard.SetLeaseOwner(assignedTo)
		}
		if checkpoint, found := data[chk.SequenceNumberKey]; found && checkpoint != "" {
			shard.SetCheckpoint(checkpoint)
		}
	}

	c.log.Debugf("Lease sync completed. Next lease sync will occur in %s", syncInterval)
	return nil
}
