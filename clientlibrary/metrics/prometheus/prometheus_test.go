package prometheus

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ODudek/go-kcl/logger"
)

func newTestLogger() logger.Logger {
	return logger.GetDefaultLogger()
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestNewMonitoringService_BackwardCompat(t *testing.T) {
	svc := NewMonitoringService(":0", "us-east-1", newTestLogger())

	assert.NotNil(t, svc)
	assert.Equal(t, ":0", svc.listenAddress)
	assert.Equal(t, "us-east-1", svc.region)
	assert.True(t, svc.startServer)
	assert.Equal(t, prom.DefaultRegisterer, svc.registerer)
	assert.Equal(t, prom.DefaultGatherer, svc.gatherer)
}

func TestNewMonitoringServiceWithOptions_Standalone(t *testing.T) {
	log := newTestLogger()
	svc := NewMonitoringServiceWithOptions(
		WithListenAddress(":9090"),
		WithRegion("eu-west-1"),
		WithLogger(log),
	)

	assert.NotNil(t, svc)
	assert.Equal(t, ":9090", svc.listenAddress)
	assert.Equal(t, "eu-west-1", svc.region)
	assert.True(t, svc.startServer)
}

func TestNewMonitoringServiceWithOptions_ExternalRegistry(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-west-2"),
		WithLogger(newTestLogger()),
	)

	assert.NotNil(t, svc)
	assert.False(t, svc.startServer)
	assert.Equal(t, prom.Registerer(reg), svc.registerer)
	assert.Equal(t, prom.Gatherer(reg), svc.gatherer)
}

func TestNewMonitoringServiceWithOptions_WithRegisterer(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegisterer(reg),
		WithRegion("ap-southeast-1"),
		WithLogger(newTestLogger()),
	)

	assert.NotNil(t, svc)
	assert.False(t, svc.startServer)
	assert.Equal(t, prom.Registerer(reg), svc.registerer)
}

func TestWithRegistry_NilIgnored(t *testing.T) {
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(nil),
		WithLogger(newTestLogger()),
	)

	assert.Equal(t, prom.DefaultRegisterer, svc.registerer, "nil registry should be ignored")
	assert.True(t, svc.startServer, "startServer should remain true when nil registry ignored")
}

func TestDefaultLogger_WhenOmitted(t *testing.T) {
	svc := NewMonitoringServiceWithOptions(
		WithListenAddress(":9090"),
	)

	assert.NotNil(t, svc.logger, "default logger should be set when WithLogger is not used")
}

func TestInit_ExternalRegistry(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)

	err := svc.Init("testapp", "my-stream", "worker-1")
	require.NoError(t, err)

	// Touch every metric so that Gather() returns them (Vec collectors
	// only appear after at least one label set is observed).
	svc.IncrRecordsProcessed("shard-0", 1)
	svc.IncrBytesProcessed("shard-0", 1)
	svc.MillisBehindLatest("shard-0", 0)
	svc.LeaseGained("shard-0")
	svc.LeaseRenewed("shard-0")
	svc.RecordGetRecordsTime("shard-0", 0)
	svc.RecordProcessRecordsTime("shard-0", 0)

	families, err := reg.Gather()
	require.NoError(t, err)

	names := metricFamilyNames(families)
	assert.Contains(t, names, "testapp_processed_bytes")
	assert.Contains(t, names, "testapp_processed_records")
	assert.Contains(t, names, "testapp_behind_latest_millis")
	assert.Contains(t, names, "testapp_leases_held")
	assert.Contains(t, names, "testapp_lease_renewals")
	assert.Contains(t, names, "testapp_get_records_duration_milliseconds")
	assert.Contains(t, names, "testapp_process_records_duration_milliseconds")
}

func TestInit_DuplicateRegistration_Tolerated(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)

	require.NoError(t, svc.Init("dupapp", "stream", "w1"))

	svc2 := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)
	err := svc2.Init("dupapp", "stream", "w2")
	assert.NoError(t, err, "re-registration with identical collectors should be tolerated")
}

func TestMetricRecording_ExternalRegistry(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)
	require.NoError(t, svc.Init("rectest", "stream-1", "worker-1"))

	svc.IncrRecordsProcessed("shard-0", 5)
	svc.IncrRecordsProcessed("shard-0", 3)
	svc.IncrBytesProcessed("shard-0", 1024)
	svc.MillisBehindLatest("shard-0", 42.5)
	svc.LeaseGained("shard-0")
	svc.LeaseRenewed("shard-0")
	svc.RecordGetRecordsTime("shard-0", 150.0)
	svc.RecordProcessRecordsTime("shard-0", 75.0)

	families, err := reg.Gather()
	require.NoError(t, err)
	byName := indexFamilies(families)

	assertCounterValue(t, byName, "rectest_processed_records", 8)
	assertCounterValue(t, byName, "rectest_processed_bytes", 1024)
	assertGaugeValue(t, byName, "rectest_behind_latest_millis", 42.5)
	assertGaugeValue(t, byName, "rectest_leases_held", 1)
	assertCounterValue(t, byName, "rectest_lease_renewals", 1)
}

func TestMetricRecording_LeaseLost(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)
	require.NoError(t, svc.Init("leasetest", "stream-1", "worker-1"))

	svc.LeaseGained("shard-0")
	svc.LeaseGained("shard-0")
	svc.LeaseLost("shard-0")

	families, err := reg.Gather()
	require.NoError(t, err)
	byName := indexFamilies(families)

	assertGaugeValue(t, byName, "leasetest_leases_held", 1)
}

func TestDeleteMetricMillisBehindLatest(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)
	require.NoError(t, svc.Init("deltest", "stream-1", "worker-1"))

	svc.MillisBehindLatest("shard-0", 100)
	svc.DeleteMetricMillisBehindLatest("shard-0")

	families, err := reg.Gather()
	require.NoError(t, err)
	byName := indexFamilies(families)

	_, exists := byName["deltest_behind_latest_millis"]
	assert.False(t, exists, "metric family should be absent after deletion")
}

func TestStart_ExternalRegistry_NoServer(t *testing.T) {
	reg := prom.NewRegistry()
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(reg),
		WithRegion("us-east-1"),
		WithLogger(newTestLogger()),
	)

	require.NoError(t, svc.Start())
	assert.Nil(t, svc.server, "no server should be created for external registry")
}

func TestStart_Standalone_ServesMetrics(t *testing.T) {
	addr := freePort(t)
	reg := prom.NewRegistry()

	svc := &MonitoringService{
		listenAddress: addr,
		region:        "us-east-1",
		logger:        newTestLogger(),
		registerer:    reg,
		gatherer:      reg,
		startServer:   true,
	}
	require.NoError(t, svc.Init("srvtest", "stream-1", "worker-1"))
	require.NoError(t, svc.Start())
	defer svc.Shutdown()

	svc.IncrRecordsProcessed("shard-0", 1)

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 50*time.Millisecond)
}

func TestShutdown_Graceful(t *testing.T) {
	addr := freePort(t)
	reg := prom.NewRegistry()

	svc := &MonitoringService{
		listenAddress: addr,
		region:        "us-east-1",
		logger:        newTestLogger(),
		registerer:    reg,
		gatherer:      reg,
		startServer:   true,
	}
	require.NoError(t, svc.Init("sdtest", "stream-1", "worker-1"))
	require.NoError(t, svc.Start())

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 50*time.Millisecond)

	svc.Shutdown()

	_, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
	assert.Error(t, err, "server should be stopped after shutdown")
}

func TestShutdown_NilServer(t *testing.T) {
	svc := NewMonitoringServiceWithOptions(
		WithRegistry(prom.NewRegistry()),
		WithLogger(newTestLogger()),
	)
	// Should not panic.
	svc.Shutdown()
}

// --- helpers ---

func metricFamilyNames(families []*dto.MetricFamily) []string {
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names
}

func indexFamilies(families []*dto.MetricFamily) map[string]*dto.MetricFamily {
	m := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		m[f.GetName()] = f
	}
	return m
}

func assertCounterValue(t *testing.T, families map[string]*dto.MetricFamily, name string, expected float64) {
	t.Helper()
	fam, ok := families[name]
	require.True(t, ok, "metric family %q not found", name)
	require.NotEmpty(t, fam.GetMetric())
	actual := fam.GetMetric()[0].GetCounter().GetValue()
	assert.InDelta(t, expected, actual, 0.001, "counter %s", name)
}

func assertGaugeValue(t *testing.T, families map[string]*dto.MetricFamily, name string, expected float64) {
	t.Helper()
	fam, ok := families[name]
	require.True(t, ok, "metric family %q not found", name)
	require.NotEmpty(t, fam.GetMetric())
	actual := fam.GetMetric()[0].GetGauge().GetValue()
	assert.InDelta(t, expected, actual, 0.001, "gauge %s", name)
}
