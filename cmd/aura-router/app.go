package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/vantutran2k1/aura/internal/router/parser"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type config struct {
	natsAddress   string
	rawLogSubject string
	queueGroup    string
	pprofPort     string
	metricsPort   string
	numWorkers    int
	jobQueueSize  int
}

type app struct {
	config     config
	nc         *nats.Conn
	tp         *sdktrace.TracerProvider
	workerPool *parser.WorkerPool
	sub        *nats.Subscription
}

func newApp(ctx context.Context) (*app, error) {
	cfg := config{
		natsAddress:   "nats://localhost:4222",
		rawLogSubject: "aura.raw.logs",
		queueGroup:    "aura-router-group",
		pprofPort:     "6062",
		metricsPort:   "9093",
		numWorkers:    50,
		jobQueueSize:  10000,
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

	workerPool := parser.NewWorkerPool(cfg.numWorkers, cfg.jobQueueSize, nc, tracer)
	workerPool.Start()

	go pprof.StartServer("localhost:" + cfg.pprofPort)
	go metrics.StartMetricsServer("localhost:" + cfg.metricsPort)

	return &app{
		config:     cfg,
		nc:         nc,
		tp:         tp,
		workerPool: workerPool,
	}, nil
}

func (a *app) run(ctx context.Context) error {
	log.Printf("subscriberd to '%s' with queue group '%s'", a.config.rawLogSubject, a.config.queueGroup)

	natsHandler := func(msg *nats.Msg) {
		job := parser.Job{
			Msg: msg,
			Ctx: context.Background(),
		}
		a.workerPool.Submit(job)
	}

	sub, err := a.nc.QueueSubscribe(a.config.rawLogSubject, a.config.queueGroup, natsHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to nats: %w", err)
	}
	a.sub = sub

	<-ctx.Done()
	log.Println("nats subscription stopping")
	return nil
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down router...")

	if a.sub != nil {
		if err := a.sub.Drain(); err != nil {
			log.Printf("nats drain error: %v", err)
		}
	}

	a.workerPool.Stop()

	a.nc.Close()

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-router shut down gracefully")
	return nil
}
