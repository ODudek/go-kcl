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

* [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)
* The v2 SDK requires a minimum version of `Go 1.21`.
* [gosec](https://github.com/securego/gosec)

### Build & Run

1. Initialize Project

2. Build
    > `make build`

3. Test
    > `make test`

## Checkpointer Backends

Go-KCL uses a pluggable `Checkpointer` interface for lease management and progress tracking. You can swap backends via `worker.WithCheckpointer()`.

### DynamoDB (default)

The default backend. No extra setup needed — the worker creates its own DynamoDB client and lease table automatically. The table name defaults to `ApplicationName`.

```go
import (
    cfg "github.com/ODudek/go-kcl/clientlibrary/config"
    wk  "github.com/ODudek/go-kcl/clientlibrary/worker"
)

kclConfig := cfg.NewKinesisClientLibConfig("my-app", "my-stream", "us-east-1", "worker-1")
worker := wk.NewWorker(factory, kclConfig)
```

You can point to a custom DynamoDB endpoint (e.g. LocalStack):

```go
kclConfig.WithDynamoDBEndpoint("http://localhost:4566")
```

Or inject a pre-configured DynamoDB checkpointer:

```go
import chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"

checkpointer := chk.NewDynamoCheckpoint(kclConfig).WithDynamoDB(dynamoClient)
worker := wk.NewWorker(factory, kclConfig).
    WithCheckpointer(checkpointer)
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
| `WithLogger(l)` | Custom logger (defaults to Logrus standard logger) |
| `WithRegistry(reg)` | Use a custom `*prometheus.Registry`; disables the built-in server |
| `WithRegisterer(r)` | Use a custom `prometheus.Registerer`; disables the built-in server |

## Examples

Working examples are available in the [`examples/`](examples/) directory:

| Example | Backend | Description |
|---|---|---|
| [`dynamodb-consumer`](examples/dynamodb-consumer/) | DynamoDB | Basic Kinesis consumer with default DynamoDB checkpointer |
| [`redis-consumer`](examples/redis-consumer/) | Redis | Basic Kinesis consumer with Redis checkpointer |
| [`redis-multitenant`](examples/redis-multitenant/) | Redis | Two applications sharing one Redis instance |
| [`prometheus-metrics`](examples/prometheus-metrics/) | Prometheus | Consumer with external Prometheus registry |

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
