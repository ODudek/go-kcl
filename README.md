# Go KCL

![technology Go](https://img.shields.io/badge/technology-go-blue.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/ODudek/go-kcl)](https://goreportcard.com/report/github.com/ODudek/go-kcl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![go-kcl](https://github.com/ODudek/go-kcl/actions/workflows/ci.yml/badge.svg)](https://github.com/ODudek/go-kcl/actions/workflows/ci.yml)

## Overview

Go-KCL is a native open-source Go library for Amazon Kinesis Data Stream (KDS) consumption. It allows developers
to program KDS consumers in lightweight Go language and still take advantage of the features presented by the native
KDS Java API libraries.

[go-kcl](https://github.com/ODudek/go-kcl) is a fork of the original [vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2), which is no longer actively maintained. This project uses [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2).

## Try it out

### Prerequisites

* `Go 1.26` or newer.
* [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)
* [gosec](https://github.com/securego/gosec)

### Build & Run

1. Initialize Project

2. Build
    > `make build`

3. Test
    > `make test`

## Building a Worker

A worker needs a Kinesis client and a checkpointer, both set explicitly; `Start` returns
`worker.ErrNoKinesisClient` or `worker.ErrNoCheckpointer` when one is missing. The library does not build AWS
clients itself, so an application links only the AWS packages it uses.

Pass any `*kinesis.Client` you build, or let the optional `awsclients` package build one from the KCL
configuration (region, credentials, `WithKinesisEndpoint`) on top of the AWS SDK's default credential chain:

```go
import (
    "github.com/ODudek/go-kcl/clientlibrary/awsclients"
    cfg "github.com/ODudek/go-kcl/clientlibrary/config"
    wk  "github.com/ODudek/go-kcl/clientlibrary/worker"
)

kclConfig := cfg.NewKinesisClientLibConfig("my-app", "my-stream", "us-east-1", "worker-1")

kinesisClient, err := awsclients.NewKinesisClient(ctx, kclConfig)
if err != nil {
    return err
}

worker := wk.NewWorker(factory, kclConfig).
    WithKinesis(kinesisClient).
    WithCheckpointer(checkpointer) // see Checkpointer Backends
```

`awsclients` links `aws-sdk-go-v2/config` and its credential providers; skip it if you already build AWS clients.

## Checkpointer Backends

Go-KCL uses a pluggable `Checkpointer` interface for lease management and progress tracking, set with
`worker.WithCheckpointer()`.

### DynamoDB

The checkpointer creates its lease table if missing. The table name defaults to `ApplicationName`.

```go
import (
    "github.com/ODudek/go-kcl/clientlibrary/awsclients"
    dynamochk "github.com/ODudek/go-kcl/clientlibrary/checkpoint/dynamodb"
)

dynamoClient, err := awsclients.NewDynamoDBClient(ctx, kclConfig) // or your own DynamoDB client
if err != nil {
    return err
}

checkpointer := dynamochk.NewDynamoCheckpoint(kclConfig).WithDynamoDB(dynamoClient)
```

For a custom endpoint (e.g. LocalStack), set it before building the client:

```go
kclConfig.WithDynamoDBEndpoint("http://localhost:4566")
```

### Redis

An alternative backend using Redis for lease management. Useful when you want lower latency, reduced AWS costs, or already run Redis in your infrastructure. Atomic lease operations are implemented via Lua scripts (equivalent to DynamoDB conditional writes).

```go
import (
    cfg      "github.com/ODudek/go-kcl/clientlibrary/config"
    redischk "github.com/ODudek/go-kcl/clientlibrary/checkpoint/redis"
    wk       "github.com/ODudek/go-kcl/clientlibrary/worker"
)

kclConfig := cfg.NewKinesisClientLibConfig("my-app", "my-stream", "us-east-1", "worker-1")

checkpointer := redischk.NewRedisCheckpoint(kclConfig, redischk.RedisConfig{
    Address:  "localhost:6379",       // host:port (required)
    Password: os.Getenv("REDIS_PWD"), // optional
    DB:       0,                      // database number 0-15
    TLS:      false,                  // enable TLS
    KeyPrefix: "kcl",                // key prefix (default: "kcl")
})

worker := wk.NewWorker(factory, kclConfig).
    WithKinesis(kinesisClient).
    WithCheckpointer(checkpointer)
```

#### Configuration

| Field | Default | Description |
|---|---|---|
| `Address` | *(required)* | `host:port` or URL (`redis://`, `rediss://`) |
| `Password` | `""` | AUTH password (overrides URL password if set) |
| `DB` | `0` | Database number 0-15 (overrides URL db if set) |
| `KeyPrefix` | `"kcl"` | Prefix for all Redis keys |
| `TLS` | `false` | Enable TLS (min TLS 1.2). Auto-enabled by `rediss://` scheme |

#### Multi-tenancy

Multiple go-kcl applications can safely share a single Redis instance. All keys are namespaced using the application's `TableName` (which defaults to `ApplicationName`):

```
kcl:{tableName}:shard:{shardID}   — per-shard lease hash
kcl:{tableName}:shards            — shard registry set
```

For example, two apps `orders` and `events` produce completely isolated keys:

```
kcl:orders:shard:shardId-000000000001
kcl:events:shard:shardId-000000000001
```

#### Features

- Atomic lease acquisition and renewal via Lua scripts
- Conditional lease owner removal (prevents accidental overwrite)
- Lease stealing support (same as DynamoDB backend)
- Sub-millisecond latency for all operations
- All `Checkpointer` interface methods supported

## Logging

The KCL logs through the `logger.Logger` interface, set with `kclConfig.WithLogger()`. The default writes to
`slog.Default()` (looked up at each call, so `slog.SetDefault` takes effect even after the config was created).
Adapters wrap a logger you have already configured; each lives in its own package, so only the one you import is
linked:

| Logger | Constructor |
|---|---|
| `log/slog` | `logger.NewSlogLogger(*slog.Logger)` |
| [logrus](https://github.com/sirupsen/logrus) | `logrus.NewLogrusLogger(logrus.FieldLogger)` from `logger/logrus` |
| [zap](https://github.com/uber-go/zap) | `zap.NewZapLogger(*zap.SugaredLogger)` from `logger/zap` |
| [zerolog](https://github.com/rs/zerolog) | `zerolog.NewZerologLogger(zerolog.Logger)` from `logger/zerolog` |

Log files and their rotation are left to the application's logger.

## KPL Aggregation

Records written by the Kinesis Producer Library in its
[aggregated format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) are
deaggregated before `ProcessRecords`: each user record keeps the sequence number, arrival time and encryption type of
the Kinesis record that carried it. Other records are passed through unchanged. An aggregated record that is
malformed (it has the KPL header and a valid checksum but does not parse) is passed to `ProcessRecords` unchanged and
logged at error level; the rest of the batch is unaffected.

## Prometheus Metrics

Go-KCL ships with a Prometheus `MonitoringService` that exposes consumer metrics (records processed, bytes processed, millis behind latest, leases held, lease renewals, get/process records duration).

### Standalone mode (default)

KCL registers metrics on the global Prometheus registry and starts its own HTTP server:

```go
import prommetrics "github.com/ODudek/go-kcl/clientlibrary/metrics/prometheus"

metricsService := prommetrics.NewMonitoringService(":2112", "us-east-1", log)
kclConfig.WithMonitoringService(metricsService)
```

### External registry

When your application already exposes a Prometheus `/metrics` endpoint, pass your own registry. KCL will register its collectors there and will **not** start a second HTTP server:

```go
import (
    prom        "github.com/prometheus/client_golang/prometheus"
    prommetrics "github.com/ODudek/go-kcl/clientlibrary/metrics/prometheus"
)

registry := prom.NewRegistry()

metricsService := prommetrics.NewMonitoringServiceWithOptions(
    prommetrics.WithRegistry(registry),
    prommetrics.WithRegion("us-east-1"),
    prommetrics.WithLogger(log),
)
kclConfig.WithMonitoringService(metricsService)

// Expose `registry` through your own HTTP handler.
```

### Available options

| Option | Description |
|---|---|
| `WithListenAddress(addr)` | Address for the standalone metrics server (default `:8080`) |
| `WithRegion(region)` | AWS region label |
| `WithLogger(l)` | Custom logger (defaults to `logger.GetDefaultLogger()`, on `log/slog`) |
| `WithRegistry(reg)` | Use a custom `*prometheus.Registry`; disables the built-in server |
| `WithRegisterer(r)` | Use a custom `prometheus.Registerer`; disables the built-in server |

## Examples

Working examples are available in the [`examples/`](examples/) directory:

| Example | Backend | Description |
|---|---|---|
| [`dynamodb-consumer`](examples/dynamodb-consumer/) | DynamoDB | Basic Kinesis consumer with a DynamoDB checkpointer |
| [`redis-consumer`](examples/redis-consumer/) | Redis | Basic Kinesis consumer with Redis checkpointer |
| [`redis-multitenant`](examples/redis-multitenant/) | Redis | Two applications sharing one Redis instance |
| [`prometheus-metrics`](examples/prometheus-metrics/) | Prometheus | Consumer with external Prometheus registry |

## Error Handling in ProcessRecords

`IRecordProcessor.ProcessRecords` returns `error`. This is the primary mechanism for controlling shard iterator advancement:

- **Return `nil`** — batch processed successfully. The consumer checkpoints and advances to the next batch.
- **Return `error`** — batch failed. The consumer **does not advance the shard iterator** and exits `getRecords`, which releases the shard lease. On the next lease acquisition the consumer restarts from the last checkpoint and retries the same batch.

```go
func (p *myProcessor) ProcessRecords(input *kc.ProcessRecordsInput) error {
    for _, record := range input.Records {
        if err := process(record); err != nil {
            // Returning the error causes the entire batch to be retried.
            return fmt.Errorf("processing record %s: %w", aws.ToString(record.SequenceNumber), err)
        }
    }

    // Checkpoint after successful processing
    lastSeq := input.Records[len(input.Records)-1].SequenceNumber
    if err := input.Checkpointer.Checkpoint(lastSeq); err != nil {
        return fmt.Errorf("checkpoint failed: %w", err)
    }
    return nil
}
```

> **Note:** Records before the failed one that were already processed will be retried on the next attempt. Make your processor idempotent (e.g. use conditional writes).

## Migrating to 0.6

0.6 removes dependencies the library pulled into every application (see [#12](https://github.com/ODudek/go-kcl/issues/12)):

| Before | Now |
|---|---|
| `wk.NewWorker(f, cfg)` built a Kinesis client and a DynamoDB checkpointer | Set both: `.WithKinesis(kc).WithCheckpointer(cp)`; build clients yourself or with `awsclients` |
| `chk.NewDynamoCheckpoint(cfg)` in `clientlibrary/checkpoint` | `dynamochk.NewDynamoCheckpoint(cfg).WithDynamoDB(client)` in `clientlibrary/checkpoint/dynamodb` |
| `logger.NewLogrusLogger(l)` | `logrus.NewLogrusLogger(l)` in `logger/logrus` |
| `logger.NewLogrusLoggerWithConfig`, `zap.NewZapLoggerWithConfig`, `zerolog.NewZerologLoggerWithConfig`, `logger.Configuration` | Configure the logger yourself and wrap it: `NewLogrusLogger`, `NewZapLogger`, `NewZerologLogger(zerolog.Logger)`, `logger.NewSlogLogger` |
| `logger.GetDefaultLogger()` was logrus | It is `log/slog` (`slog.Default()`) |

## Documentation

Go-KCL matches exactly the same interface and programming model from original Amazon KCL, the best place for getting reference, tutorial is from Amazon itself:

* [Developing Consumers Using the Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
* [Troubleshooting](https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html)
* [Advanced Topics](https://docs.aws.amazon.com/streams/latest/dev/advanced-consumers.html)

## Contributing

The go-kcl project team welcomes contributions from the community. Before you start working with go-kcl, please
read our [Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository must be
signed as described on that page. Your signature certifies that you wrote the patch or have the right to pass it on
as an open-source patch. For more detailed information, refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License
