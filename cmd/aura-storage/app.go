package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	storagegrpc "github.com/vantutran2k1/aura/internal/storage/grpc"
	"github.com/vantutran2k1/aura/internal/storage/writer"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
)

type config struct {
	natsAddress       string
	clickhouseAddress string
	postgresURL       string

	logsSubject    string
	metricsSubject string
	tracesSubject  string

	grpcPort    string
	pprofPort   string
	metricsPort string

	logsBatchSize     int
	logsFlushInterval time.Duration

	metricsBatchSize     int
	metricsFlushInterval time.Duration

	tracesBatchSize     int
	tracesFlushInterval time.Duration
}

type app struct {
	config             config
	tp                 *sdktrace.TracerProvider
	chConn             clickhouse.Conn
	dbPool             *pgxpool.Pool
	nc                 *nats.Conn
	logsBatchWriter    *writer.BatchWriter
	metricsBatchWriter *writer.MetricsBatchWriter
	tracesBatchWriter  *writer.TracesBatchWriter
	grpcServer         *grpc.Server
	subLogs            *nats.Subscription
	subMetrics         *nats.Subscription
	subTraces          *nats.Subscription
}

func newApp(ctx context.Context) (*app, error) {
	cfg := config{
		natsAddress:       "nats://localhost:4222",
		clickhouseAddress: "localhost:9000",
		postgresURL:       "postgres://user:password@localhost:5432/aura",

		logsSubject:    "aura.processed.logs",
		metricsSubject: "aura.processed.metrics",
		tracesSubject:  "aura.processed.traces",

		grpcPort:    ":50051",
		pprofPort:   "6063",
		metricsPort: "9094",

		logsBatchSize:     1000,
		logsFlushInterval: 1 * time.Second,

		metricsBatchSize:     500,
		metricsFlushInterval: 1 * time.Second,

		tracesBatchSize:     500,
		tracesFlushInterval: 1 * time.Second,
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-storage")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}

	chConn, err := connectClickHouse(ctx, cfg.clickhouseAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}
	log.Println("connected to clickhouse")

	dbPool, err := pgxpool.New(ctx, cfg.postgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	if err := dbPool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	log.Println("connected to postgresql (timescaledb)")

	nc, err := nats.Connect(cfg.natsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("connected to nats")

	chWriter := writer.NewClickHouseWriter(chConn)
	batchConfig := writer.BatchWriterConfig{
		BatchSize:     cfg.logsBatchSize,
		FlushInterval: cfg.logsFlushInterval,
	}
	batchWriter := writer.NewBatchWriter(ctx, chWriter, batchConfig)
	log.Println("[logs] batch writer started")

	tsdbWriter := writer.NewTimescaleDBWriter(dbPool)
	metricsBatchConfig := writer.MetricsBatchWriterConfig{
		BatchSize:     cfg.metricsBatchSize,
		FlushInterval: cfg.metricsFlushInterval,
	}
	metricsBatchWriter := writer.NewMetricsBatchWriter(ctx, tsdbWriter, metricsBatchConfig)
	log.Println("[metrics] batch writer started")

	chTraceWriter := writer.NewClickHouseTraceWriter(chConn)
	tracesBatchConfig := writer.TracesBatchWriterConfig{BatchSize: cfg.tracesBatchSize, FlushInterval: cfg.tracesFlushInterval}
	tracesBatchWriter := writer.NewTracesBatchWriter(ctx, chTraceWriter, tracesBatchConfig)
	log.Println("[traces] batch writer started")

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	storageServer := storagegrpc.NewServer(chConn, dbPool)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)
	reflection.Register(grpcServer)

	go pprof.StartServer("localhost:" + cfg.pprofPort)
	go metrics.StartMetricsServer("localhost:" + cfg.metricsPort)

	return &app{
		config:             cfg,
		tp:                 tp,
		chConn:             chConn,
		dbPool:             dbPool,
		nc:                 nc,
		logsBatchWriter:    batchWriter,
		metricsBatchWriter: metricsBatchWriter,
		tracesBatchWriter:  tracesBatchWriter,
		grpcServer:         grpcServer,
	}, nil
}

func (a *app) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Printf("grpc server listening on %s", a.config.grpcPort)
		lis, err := net.Listen("tcp", a.config.grpcPort)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}

		go func() {
			if err := a.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
				log.Printf("grpc server error: %v", err)
			}
		}()

		<-ctx.Done()
		log.Println("shutting down grpc server...")
		a.grpcServer.GracefulStop()

		return nil
	})

	g.Go(func() error {
		logsHandler := func(msg *nats.Msg) {
			var logMsg pb.Log
			if err := proto.Unmarshal(msg.Data, &logMsg); err != nil {
				log.Printf("[logs] error unmarshling: %v\n", err)
				return
			}
			if err := a.logsBatchWriter.AddLog(&logMsg); err != nil {
				log.Printf("[logs] error adding to batch %v\n", err)
			}
		}

		sub, err := a.nc.Subscribe(a.config.logsSubject, logsHandler)
		if err != nil {
			return fmt.Errorf("[logs] failed to subscribe to nats: %w", err)
		}
		a.subLogs = sub
		log.Printf("[logs] subscribed to nats: %s", a.config.logsSubject)

		<-ctx.Done()
		log.Println("[logs] draining nats subscription...")
		if err := a.subLogs.Drain(); err != nil {
			return fmt.Errorf("[logs] nats drain error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		metricsHandler := func(msg *nats.Msg) {
			var metric pb.Metric
			if err := proto.Unmarshal(msg.Data, &metric); err != nil {
				log.Printf("[metrics] error unmarshaling: %v\n", err)
				return
			}
			if err := a.metricsBatchWriter.AddMetric(&metric); err != nil {
				log.Printf("[metrics] error adding to batch: %v\n", err)
			}
		}

		sub, err := a.nc.Subscribe(a.config.metricsSubject, metricsHandler)
		if err != nil {
			return fmt.Errorf("[metrics] failed to subscribe to nats: %w", err)
		}
		a.subMetrics = sub
		log.Printf("[metrics] subscribed to nats: %s", a.config.metricsSubject)

		<-ctx.Done()
		log.Printf("[metrics] draining nats subscription...")
		if err := a.subMetrics.Drain(); err != nil {
			return fmt.Errorf("[metrics] nats drain error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		tracesHandler := func(msg *nats.Msg) {
			var span pb.Span
			if err := proto.Unmarshal(msg.Data, &span); err != nil {
				log.Printf("[traces] error unmarshaling: %v\n", err)
				return
			}
			if err := a.tracesBatchWriter.AddSpan(&span); err != nil {
				log.Printf("[traces] error adding to batch: %v\n", err)
			}
		}
		sub, err := a.nc.Subscribe(a.config.tracesSubject, tracesHandler)
		if err != nil {
			return fmt.Errorf("[traces] failed to subscribe to nats: %w", err)
		}
		a.subTraces = sub
		log.Printf("[traces] subscribed to nats: %s", a.config.tracesSubject)
		<-ctx.Done()
		log.Println("[traces] draining nats subscription...")
		if err := a.subTraces.Drain(); err != nil {
			return fmt.Errorf("[traces] nats drain error: %w", err)
		}
		return nil
	})

	return g.Wait()
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down storage service...")

	a.logsBatchWriter.Close()
	a.metricsBatchWriter.Close()
	a.tracesBatchWriter.Close()

	a.nc.Close()
	a.chConn.Close()
	a.dbPool.Close()

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-storage service shut down gracefully")
	return nil
}

func connectClickHouse(ctx context.Context, addr string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "aura",
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "aura-storage", Version: "0.0.1"},
			},
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}
