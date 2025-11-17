package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"github.com/vantutran2k1/aura/internal/router/metrics_parser"
	"github.com/vantutran2k1/aura/internal/router/parser"
	"github.com/vantutran2k1/aura/internal/router/traces_parser"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"
)

type routerWorkersConfig struct {
	Logs    int `mapstructure:"logs"`
	Metrics int `mapstructure:"metrics"`
	Traces  int `mapstructure:"traces"`
}

type Config struct {
	PprofPort   string              `mapstructure:"pprof"`
	MetricsPort string              `mapstructure:"metrics"`
	Workers     routerWorkersConfig `mapstructure:"workers"`

	NatsAddress       string
	RawLogsSubject    string
	RawMetricsSubject string
	RawTracesSubject  string
}

type app struct {
	config            Config
	nc                *nats.Conn
	tp                *sdktrace.TracerProvider
	logWorkerPool     *parser.WorkerPool
	metricsWorkerPool *metrics_parser.WorkerPool
	tracesWorkerPool  *traces_parser.WorkerPool
	subLogs           *nats.Subscription
	subMetrics        *nats.Subscription
	subTraces         *nats.Subscription
}

func loadConfig() (Config, error) {
	v := viper.New()
	v.SetConfigFile("config.yaml")
	v.AddConfigPath(".")

	v.SetEnvPrefix("AURA")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("router.workers.logs", 50)
	v.SetDefault("router.workers.metrics", 25)
	v.SetDefault("router.workers.traces", 25)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("config.yaml not found, using defaults and env vars")
		} else {
			return Config{}, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg Config
	if err := v.UnmarshalKey("router", &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal router config: %w", err)
	}

	cfg.NatsAddress = v.GetString("nats")
	cfg.RawLogsSubject = v.GetString("subjects.logs.raw")
	cfg.RawMetricsSubject = v.GetString("subjects.metrics.raw")
	cfg.RawTracesSubject = v.GetString("subjects.traces.raw")

	log.Printf("Configuration loaded: %+v", cfg)
	return cfg, nil
}

func newApp(ctx context.Context) (*app, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-router")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}
	tracer := otel.Tracer("aura-router")

	nc, err := nats.Connect(cfg.NatsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("connected to nats")

	logWorkerPool := parser.NewWorkerPool(cfg.Workers.Logs, 10000, nc, tracer)
	logWorkerPool.Start()

	metricWorkerPool := metrics_parser.NewWorkerPool(cfg.Workers.Metrics, 5000, nc, tracer)
	metricWorkerPool.Start()

	tracesWorkerPool := traces_parser.NewWorkerPool(cfg.Workers.Traces, 5000, nc, tracer)
	tracesWorkerPool.Start()

	go pprof.StartServer(cfg.PprofPort)
	go metrics.StartMetricsServer(cfg.MetricsPort)

	return &app{
		config:            cfg,
		nc:                nc,
		tp:                tp,
		logWorkerPool:     logWorkerPool,
		metricsWorkerPool: metricWorkerPool,
		tracesWorkerPool:  tracesWorkerPool,
	}, nil
}

func (a *app) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Printf("subscribed to '%s'", a.config.RawLogsSubject)

		logsHandler := func(msg *nats.Msg) {
			job := parser.Job{
				Msg: msg,
				Ctx: context.Background(),
			}
			a.logWorkerPool.Submit(job)
		}

		sub, err := a.nc.QueueSubscribe(a.config.RawLogsSubject, "aura-router-logs-group", logsHandler)
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
		log.Printf("subscribed to '%s'", a.config.RawMetricsSubject)

		metricsHandler := func(msg *nats.Msg) {
			job := metrics_parser.Job{
				Msg: msg,
				Ctx: context.Background(),
			}
			a.metricsWorkerPool.Submit(job)
		}

		sub, err := a.nc.QueueSubscribe(a.config.RawMetricsSubject, "aura-router-metrics-group", metricsHandler)
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

	g.Go(func() error {
		log.Printf("subscribed to '%s'", a.config.RawTracesSubject)

		tracesHandler := func(msg *nats.Msg) {
			job := traces_parser.Job{
				Msg: msg,
				Ctx: context.Background(),
			}
			a.tracesWorkerPool.Submit(job)
		}

		sub, err := a.nc.QueueSubscribe(a.config.RawTracesSubject, "aura-router-traces-group", tracesHandler)
		if err != nil {
			return fmt.Errorf("failed to subscribe to traces: %w", err)
		}
		a.subTraces = sub

		<-ctx.Done()
		log.Println("[traces] draining nats subscription...")
		if err := a.subTraces.Drain(); err != nil {
			return fmt.Errorf("traces nats drain error: %w", err)
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
	a.tracesWorkerPool.Stop()

	a.nc.Close()

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-router shut down gracefully")
	return nil
}
