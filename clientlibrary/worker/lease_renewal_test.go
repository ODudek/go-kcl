package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chk "github.com/ODudek/go-kcl/clientlibrary/checkpoint"
	"github.com/ODudek/go-kcl/clientlibrary/config"
	"github.com/ODudek/go-kcl/clientlibrary/metrics"
	par "github.com/ODudek/go-kcl/clientlibrary/partition"
	"github.com/ODudek/go-kcl/logger"
)

var errStoreTimeout = errors.New("getLease script error: context deadline exceeded")

// renewalCheckpointer fails the first GetLease calls with errs, then renews
// the lease for a minute.
type renewalCheckpointer struct {
	fakeCheckpointer
	errs  []error
	calls int
}

func (c *renewalCheckpointer) GetLease(s *par.ShardStatus, _ string) error {
	c.calls++
	if c.calls <= len(c.errs) {
		return c.errs[c.calls-1]
	}
	s.SetLeaseTimeout(time.Now().Add(time.Minute))
	return nil
}

func newRenewingConsumer(cp chk.Checkpointer, leaseTimeout time.Time) *PollingShardConsumer {
	shard := &par.ShardStatus{ID: "s1", Mux: &sync.RWMutex{}}
	shard.SetLeaseTimeout(leaseTimeout)
	cfg := config.NewKinesisClientLibConfig("app", "stream", "us-east-1", "worker-1").
		WithLeaseRefreshWaitTime(10)
	return &PollingShardConsumer{
		commonShardConsumer: commonShardConsumer{
			shard:        shard,
			checkpointer: cp,
			kclConfig:    cfg,
			mService:     metrics.NoopMonitoringService{},
		},
		consumerID: "worker-1",
		mService:   metrics.NoopMonitoringService{},
	}
}

// One timeout of the checkpoint store must not stop the consumer while its
// lease is still held: it would release the lease and the shard would be read
// again from its last checkpoint.
func TestRenewLease_RetriesTransientErrorsWhileTheLeaseIsHeld(t *testing.T) {
	cp := &renewalCheckpointer{errs: []error{errStoreTimeout, errStoreTimeout}}
	sc := newRenewingConsumer(cp, time.Now().Add(time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	require.NoError(t, sc.renewLease(ctx))
	assert.Greater(t, cp.calls, 2, "renewal goes on after the failed attempts")
}

func TestRenewLease_GivesUpWhenTheLeaseExpiresBeforeTheNextAttempt(t *testing.T) {
	cp := &renewalCheckpointer{errs: []error{errStoreTimeout}}
	sc := newRenewingConsumer(cp, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	assert.ErrorIs(t, sc.renewLease(ctx), errStoreTimeout)
	assert.Equal(t, 1, cp.calls)
}

// Issue #12: renewLease logged through the global logrus logger, so its lines
// bypassed the configured logger's format, level and fields.
func TestRenewLease_LogsThroughTheConfiguredLogger(t *testing.T) {
	var buf bytes.Buffer
	cp := &renewalCheckpointer{errs: []error{errStoreTimeout}}
	sc := newRenewingConsumer(cp, time.Now())
	sc.kclConfig.WithLogger(logger.NewSlogLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = sc.renewLease(ctx)

	assert.Contains(t, buf.String(), `"level":"ERROR","msg":"Error in refreshing lease on shard: s1`)
}

func TestRenewLease_StopsAtOnceWhenTheLeaseIsLost(t *testing.T) {
	for name, lost := range map[string]error{
		"taken":   chk.ErrLeaseNotAcquired{Cause: "current lease timeout not yet expired"},
		"claimed": errors.New(chk.ErrShardClaimed),
	} {
		t.Run(name, func(t *testing.T) {
			cp := &renewalCheckpointer{errs: []error{lost}}
			sc := newRenewingConsumer(cp, time.Now().Add(time.Minute))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			assert.Equal(t, lost, sc.renewLease(ctx))
			assert.Equal(t, 1, cp.calls)
		})
	}
}
