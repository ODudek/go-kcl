package prometheus

import (
	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/ODudek/go-kcl/logger"
)

// Option configures MonitoringService via the functional options pattern.
type Option func(*config)

type config struct {
	listenAddress string
	region        string
	logger        logger.Logger
	registerer    prom.Registerer
	gatherer      prom.Gatherer
	startServer   bool
}

func defaultConfig() config {
	return config{
		listenAddress: ":8080",
		logger:        logger.GetDefaultLogger(),
		registerer:    prom.DefaultRegisterer,
		gatherer:      prom.DefaultGatherer,
		startServer:   true,
	}
}

// WithListenAddress sets the address for the standalone metrics HTTP server.
func WithListenAddress(addr string) Option {
	return func(c *config) {
		c.listenAddress = addr
	}
}

// WithRegion sets the AWS region label.
func WithRegion(region string) Option {
	return func(c *config) {
		c.region = region
	}
}

// WithLogger sets a custom logger.
func WithLogger(l logger.Logger) Option {
	return func(c *config) {
		c.logger = l
	}
}

// WithRegistry configures the service to use the given registry instead of
// the global default. When set, no standalone HTTP server is started —
// the caller is responsible for exposing the registry.
func WithRegistry(reg *prom.Registry) Option {
	return func(c *config) {
		if reg == nil {
			return
		}
		c.registerer = reg
		c.gatherer = reg
		c.startServer = false
	}
}

// WithRegisterer allows passing a lower-level prom.Registerer (e.g. a
// wrapped or prefixed registerer). When used alone (without WithRegistry)
// the gatherer stays at the default and no server is started.
func WithRegisterer(r prom.Registerer) Option {
	return func(c *config) {
		if r == nil {
			return
		}
		c.registerer = r
		c.startServer = false
	}
}
