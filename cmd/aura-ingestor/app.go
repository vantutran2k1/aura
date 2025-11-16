package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go"
	aurahttp "github.com/vantutran2k1/aura/internal/ingestor/http"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type config struct {
	apiPort            string
	pprofPort          string
	metricsPort        string
	natsAddress        string
	natsLogsSubject    string
	natsMetricsSubject string
	natsTracesSubject  string
}

type app struct {
	config     config
	nc         *nats.Conn
	httpServer *http.Server
	tp         *sdktrace.TracerProvider
}

func newApp(ctx context.Context) (*app, error) {
	cfg := config{
		apiPort:            "8080",
		pprofPort:          "6060",
		metricsPort:        "9091",
		natsAddress:        "nats://localhost:4222",
		natsLogsSubject:    "aura.raw.logs",
		natsMetricsSubject: "aura.raw.metrics",
		natsTracesSubject:  "aura.raw.traces",
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-ingestor")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}

	nc, err := nats.Connect(cfg.natsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("connected to nats")

	apiHandler := aurahttp.NewAPIHandler(
		nc,
		cfg.natsLogsSubject,
		cfg.natsMetricsSubject,
		cfg.natsTracesSubject,
	)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)
	r.Use(metricsMiddleware)
	r.Use(func(h http.Handler) http.Handler {
		return otelhttp.NewHandler(h, "aura-ingestor-http")
	})
	r.Post("/v1/logs", apiHandler.HandleLogs)
	r.Post("/v1/metrics", apiHandler.HandleMetrics)
	r.Post("/v1/traces", apiHandler.HandleTraces)

	httpServer := &http.Server{
		Addr:    ":" + cfg.apiPort,
		Handler: r,
	}

	go pprof.StartServer("localhost:" + cfg.pprofPort)
	go metrics.StartMetricsServer("localhost:" + cfg.metricsPort)

	return &app{
		config:     cfg,
		nc:         nc,
		httpServer: httpServer,
		tp:         tp,
	}, nil
}

func (a *app) run() error {
	log.Printf("aura-ingestor starting on port %s", a.config.apiPort)
	if err := a.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("http server error: %w", err)
	}
	log.Println("http server stopped")

	return nil
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down ingestor...")

	if err := a.httpServer.Shutdown(ctx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	if err := a.nc.Drain(); err != nil {
		log.Printf("nats drain error: %v", err)
	}
	a.nc.Close()

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-ingestor shut down gracefully")
	return nil
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		start := time.Now()
		defer func() {
			duration := time.Since(start)

			metrics.HTTPRequestDuration.WithLabelValues(
				"aura-ingestor",
				r.Method,
				strconv.Itoa(ww.Status()),
			).Observe(duration.Seconds())

			metrics.HTTPRequestsTotal.WithLabelValues(
				"aura-ingestor",
				r.Method,
				strconv.Itoa(ww.Status()),
			).Inc()
		}()

		next.ServeHTTP(ww, r)
	})
}
