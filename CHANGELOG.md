# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-10-27

Trims the dependencies the library linked into every application ([#12](https://github.com/ODudek/go-kcl/issues/12)). A polling consumer with the Redis checkpointer and an injected logger and Kinesis client no longer links `logrus`, `lumberjack`, `kinesis-aggregation`, `github.com/golang/protobuf`, the DynamoDB client, `aws-sdk-go-v2/config` and its credential chain, or `google/uuid`. See "Migrating to 0.6" in the README.

### Fixed

- `renewLease` logged through the global logrus logger, so its lines (`Refreshing lease…`, `Error in refreshing lease…`) bypassed the configured logger's format, level and fields. It now uses `kclConfig.Logger`.
- One malformed KPL aggregated record no longer drops its whole batch. The old deaggregator returned nothing for the batch when any record failed to parse, `ProcessRecords` was not called, and the next checkpoint skipped up to `MaxRecords` records, valid ones included. Each Kinesis record is now deaggregated on its own; a malformed one is passed to the processor unchanged and logged at error level.
- A malformed KPL aggregated record whose partition key index is out of range no longer panics the shard consumer (and the process).
- The default worker ID is a random version 4 UUID; the version 1 UUID it replaces embedded the host's MAC address.
- `Panicf` of the logrus and zap adapters called `Fatalf` (exiting the process); it panics.
- The zerolog adapter's `WithFields` dropped every field.
- Two "max retry count" errors in the polling consumer were logged with an unformatted `message` string.

### Changed

- **Breaking:** `Worker.Start` requires a Kinesis client (`WithKinesis`) and a checkpointer (`WithCheckpointer`), and returns `ErrNoKinesisClient` / `ErrNoCheckpointer` without them. It no longer builds a default Kinesis client and DynamoDB checkpointer (which called `log.Fatalf` when the AWS config failed to load).
- **Breaking:** `DynamoCheckpoint` moves from `clientlibrary/checkpoint` to `clientlibrary/checkpoint/dynamodb`. `Init` returns `ErrNoDynamoDBClient` when no client was set with `WithDynamoDB` instead of building one.
- **Breaking:** the `logger` package holds only the `Logger` interface, `Fields`, `NewSlogLogger` and `GetDefaultLogger`, whose default is now `log/slog` (`slog.Default()`, looked up at each call). The logrus adapter moves to `logger/logrus`. Adapters wrap an already configured logger: `NewLogrusLoggerWithConfig`, `NewZapLoggerWithConfig`, `NewZerologLoggerWithConfig`, `logger.Configuration` and the level constants are removed, and `zerolog.NewZerologLogger` takes a `zerolog.Logger`. Log files are left to the application, so `lumberjack` is gone.
- **Breaking:** Go 1.26 is required. Dependency floors are raised to current releases (aws-sdk-go-v2 1.47, client_golang 1.25, zap 1.28, zerolog 1.35, go-redis 9.23, protobuf 1.36).
- KPL deaggregation is implemented in-repo on `google.golang.org/protobuf/encoding/protowire`, replacing `github.com/awslabs/kinesis-aggregation`.

### Added

- `clientlibrary/awsclients`: `NewKinesisClient` and `NewDynamoDBClient` build clients from the KCL configuration (region, credentials, endpoint) on the AWS SDK's default chain, with the same retryer as before and `BaseEndpoint` instead of the deprecated `EndpointResolverWithOptions`. Optional: only applications that import it link `aws-sdk-go-v2/config`.
- `logger.NewSlogLogger` adapts a `*slog.Logger`.
- CI runs `govulncheck`; Dependabot proposes weekly grouped updates of Go modules and GitHub Actions.

## [0.5.1] - 2026-10-27

### Fixed

- A worker no longer stops consuming a shard for good after a short outage of the checkpoint store ([#11](https://github.com/ODudek/go-kcl/issues/11)). When a shard consumer exited and `RemoveLeaseOwner` failed, the store kept this worker as the lease owner; the next `FetchCheckpoint` or Redis lease sync copied that owner back into memory, and the worker skipped the shard as its own although no consumer read it, until a restart. The worker now tracks the shards its consumers read and leases every other shard again, including one whose stale lease names this worker. Redis `FetchCheckpoint` and `syncLeases` no longer make this worker an owner in memory; only `GetLease` does.
- A failed lease renewal no longer stops the shard consumer while the lease is still held: transient errors (e.g. a Redis timeout) are retried every `LeaseRefreshWaitTime` until the lease would expire before the next attempt. A lease taken or claimed by another worker still stops the consumer at once. Before, one timeout released the lease and the shard was read again from its last checkpoint.
- A failed `RemoveLeaseOwner` is logged at warn level instead of debug, unless another worker holds the lease by then.

## [0.5.0] - 2026-10-13

### Fixed

- A starting worker leases its first shard and starts reading immediately. 0.4.2 synced the shard list on startup but still slept a jittered `ShardSyncIntervalMillis` (30–90 s at the default 60 s) before the first lease, so consumption still began about a minute after start. The first `eventLoop` iteration now runs without waiting.
- A worker takes its whole fair share of free shards in one iteration, `ceil(open shards / active workers)` capped at `MaxLeasesForWorker`, instead of one shard per `ShardSyncIntervalMillis`. Before, shard k of a worker started only after about k × 60 s, so adding shards or workers did not speed consumption up. A worker counts as active while it holds an unexpired lease. Shards are visited in random order, so workers starting together spread over them, and `rebalance` evens out the rest.
- `rebalance` no longer fails with `Error in rebalance: AssignedToNotFoundForShard` whenever any shard is free (after a deploy, or while another worker's fresh lease has no checkpoint yet). Free shards are left to the normal lease path and stealing is skipped quietly at debug level. Before, this error aborted every rebalance while a shard looked free, so an uneven spread could persist for minutes.
- `FetchCheckpoint` (DynamoDB and Redis) reads the lease owner and timeout even when the shard has no checkpoint yet, so a shard another worker just leased no longer looks free. Redis `syncLeases` clears another worker's owner that Redis no longer holds (a lease released on shutdown), so a gone worker no longer counts as active. This worker's own leases are never cleared by a sync; their shard consumer renews them.

### Added

- `LeaseAcquisitionIntervalMillis` (`WithLeaseAcquisitionIntervalMillis`, default 2000): after an iteration that acquired a lease, the next shard sync runs after this jittered interval instead of the full `ShardSyncIntervalMillis`. This picks up within seconds the shards that were contended or released meanwhile (a lease another worker raced for, a stolen or child shard).

### Changed

- `ListActiveWorkers` (DynamoDB and Redis) skips unassigned shards instead of returning `ErrShardNotAssigned`, which is deprecated and no longer returned.

## [0.4.2] - 2026-05-20

### Fixed

- `Worker.eventLoop` now performs the first shard sync immediately on startup, before entering the jittered `ShardSyncIntervalMillis` loop. This removes the initial 30-90 second discovery delay with the default 60 second shard sync interval while preserving jitter for subsequent periodic syncs.

## [0.4.1] - 2026-03-26

### Fixed

- `PollingShardConsumer.getRecords` now recovers from `ExpiredIteratorException` by renewing the shard iterator from the last checkpoint instead of terminating the shard consumer. Previously, if `ProcessRecords` took longer than the 5-minute iterator TTL, the consumer would stop processing the shard permanently.

## [0.4.0] - 2026-03-24

### Fixed

- `IRecordProcessor.ProcessRecords` now returns `error` — previously the return value was void, causing go-kcl to silently advance the shard iterator even when the processor failed. Returning an error from `ProcessRecords` now causes the shard consumer to stop advancing the iterator and retry the same batch of records from the last checkpoint.

### Changed

- `commonShardConsumer.processRecords` returns `error` and propagates it to `getRecords`
- `PollingShardConsumer.getRecords` returns error from `processRecords`, triggering shard lease release and retry from checkpoint

## [0.3.0] - 2026-02-11

### Added

- Functional options pattern for Prometheus `MonitoringService` (`NewMonitoringServiceWithOptions`)
- `WithRegistry` option to use an external `*prometheus.Registry` instead of the global default
- `WithRegisterer` option for lower-level registerer injection
- `WithListenAddress`, `WithRegion`, `WithLogger` options
- Graceful HTTP server shutdown in `MonitoringService.Shutdown()`
- HTTP server timeouts (read, idle) for Slowloris protection
- Tolerance for duplicate metric registration (`AlreadyRegisteredError`)
- Prometheus metrics example (`examples/prometheus-metrics/`)

## [0.2.1] - 2026-02-11

### Fixed

- Integer overflow in MaxRecords int to int32 conversion (polling-shard-consumer.go)

## [0.2.0] - 2026-02-11

### Added

- Redis-backed checkpointer as an alternative to DynamoDB
- Changelog version bump check in GitHub Actions CI

## [0.1.0] - 2026-02-11

### Changed

- Fork from vmware/vmware-go-kcl-v2
- Update module path to github.com/ODudek/go-kcl
- Bump minimum Go version to 1.21
- Upgrade AWS SDK and dependencies
