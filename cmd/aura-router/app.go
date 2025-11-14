package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/vantutran2k1/aura/internal/router/metrics_parser"
	"github.com/vantutran2k1/aura/internal/router/parser"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"
)

type config struct {
	natsAddress string
	pprofPort   string
	metricsPort string

	rawLogSubject   string
	logQueueGroup   string
	logWorkers      int
	logJobQueueSize int

	rawMetricsSubject   string
	metricsQueueGroup   string
	metricsWorkers      int
	metricsJobQueueSize int
}

type app struct {
	config            config
	nc                *nats.Conn
	tp                *sdktrace.TracerProvider
	logWorkerPool     *parser.WorkerPool
	metricsWorkerPool *metrics_parser.WorkerPool
	subLogs           *nats.Subscription
	subMetrics        *nats.Subscription
}

func newApp(ctx context.Context) (*app, error) {
	cfg := config{
		natsAddress: "nats://localhost:4222",
		pprofPort:   "6062",
		metricsPort: "9093",

		rawLogSubject:   "aura.raw.logs",
		logQueueGroup:   "aura-router-group",
		logWorkers:      50,
		logJobQueueSize: 10000,

		rawMetricsSubject:   "aura.raw.metrics",
		metricsQueueGroup:   "aura-router-metrics-group",
		metricsWorkers:      25,
		metricsJobQueueSize: 5000,
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-router")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}
	tracer := otel.Tracer("aura-router")

	nc, err := nats.Connect(cfg.natsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("connected to nats")

	logWorkerPool := parser.NewWorkerPool(cfg.logWorkers, cfg.logJobQueueSize, nc, tracer)
	logWorkerPool.Start()

	metricWorkerPool := metrics_parser.NewWorkerPool(cfg.metricsWorkers, cfg.metricsJobQueueSize, nc, tracer)
	metricWorkerPool.Start()

	go pprof.StartServer("localhost:" + cfg.pprofPort)
	go metrics.StartMetricsServer("localhost:" + cfg.metricsPort)

	return &app{
		config:            cfg,
		nc:                nc,
		tp:                tp,
		logWorkerPool:     logWorkerPool,
		metricsWorkerPool: metricWorkerPool,
	}, nil
}

func (a *app) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Printf("subscriberd to '%s' with queue group '%s'", a.config.rawLogSubject, a.config.logQueueGroup)

		logsHandler := func(msg *nats.Msg) {
			job := parser.Job{
				Msg: msg,
				Ctx: context.Background(),
			}
			a.logWorkerPool.Submit(job)
		}

		sub, err := a.nc.QueueSubscribe(a.config.rawLogSubject, a.config.logQueueGroup, logsHandler)
		if err != nil {
			return fmt.Errorf("failed to subscribe to logs: %w", err)
		}
		a.subLogs = sub

		<-ctx.Done()
		log.Println("[logs] draining nats subscription...")
		if err := a.subLogs.Drain(); err != nil {
			return fmt.Errorf("logs nats drain error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		log.Printf("subscribed to '%s' with queue group '%s'", a.config.rawMetricsSubject, a.config.metricsQueueGroup)

		metricsHandler := func(msg *nats.Msg) {
			job := metrics_parser.Job{
				Msg: msg,
				Ctx: context.Background(),
			}
			a.metricsWorkerPool.Submit(job)
		}

		sub, err := a.nc.QueueSubscribe(a.config.rawMetricsSubject, a.config.metricsQueueGroup, metricsHandler)
		if err != nil {
			return fmt.Errorf("failed to subscribe to metrics: %w", err)
		}
		a.subMetrics = sub

		<-ctx.Done()
		log.Println("[metrics] draining nats subscription...")
		if err := a.subMetrics.Drain(); err != nil {
			return fmt.Errorf("metrics nats drain error: %w", err)
		}
		return nil
	})

	log.Println("aura-router is running with logs and metrics processors")
	return g.Wait()
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down router...")

	a.logWorkerPool.Stop()
	a.metricsWorkerPool.Stop()

	a.nc.Close()

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-router shut down gracefully")
	return nil
}
