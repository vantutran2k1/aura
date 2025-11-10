package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var HTTPRequestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "aura_http_requests_total",
		Help: "Total number of HTTP requests",
	},
	[]string{"service", "method", "code"},
)

var HTTPRequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "aura_http_request_duration_seconds",
		Help:    "Histogram of HTTP request latencies",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "method", "code"},
)

var CacheRequestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "aura_cache_requests_total",
		Help: "Total number of cache requests",
	},
	[]string{"service", "outcome"},
)

func StartMetricsServer(addr string) {
	log.Printf("starting metrics server on %s", addr)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("error: metrics server failed: %v", err)
	}
}
