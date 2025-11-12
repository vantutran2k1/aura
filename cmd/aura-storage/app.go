package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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
	natsSubject       string
	clickhouseAddress string
	grpcPort          string
	pprofPort         string
	metricsPort       string
	batchSize         int
	flushInterval     time.Duration
}

type app struct {
	config      config
	tp          *sdktrace.TracerProvider
	chConn      clickhouse.Conn
	nc          *nats.Conn
	batchWriter *writer.BatchWriter
	grpcServer  *grpc.Server
	sub         *nats.Subscription
}

func newApp(ctx context.Context) (*app, error) {
	cfg := config{
		natsAddress:       "nats://localhost:4222",
		natsSubject:       "aura.processed.logs",
		clickhouseAddress: "localhost:9000",
		grpcPort:          ":50051",
		pprofPort:         "6063",
		metricsPort:       "9094",
		batchSize:         1000,
		flushInterval:     1 * time.Second,
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

	nc, err := nats.Connect(cfg.natsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("connected to nats")

	chWriter := writer.NewClickHouseWriter(chConn)
	batchConfig := writer.BatchWriterConfig{
		BatchSize:     cfg.batchSize,
		FlushInterval: cfg.flushInterval,
	}
	batchWriter := writer.NewBatchWriter(ctx, chWriter, batchConfig)
	log.Println("batch writer started")

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	storageServer := storagegrpc.NewServer(chConn)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)
	reflection.Register(grpcServer)

	go pprof.StartServer("localhost:" + cfg.pprofPort)
	go metrics.StartMetricsServer("localhost:" + cfg.metricsPort)

	return &app{
		config:      cfg,
		tp:          tp,
		chConn:      chConn,
		nc:          nc,
		batchWriter: batchWriter,
		grpcServer:  grpcServer,
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
		natsHandler := func(msg *nats.Msg) {
			var logMsg pb.Log
			if err := proto.Unmarshal(msg.Data, &logMsg); err != nil {
				log.Printf("error unmarshling log message: %v\n", err)
				return
			}
			if err := a.batchWriter.AddLog(&logMsg); err != nil {
				log.Printf("error adding log to batch %v\n", err)
			}
		}

		sub, err := a.nc.Subscribe(a.config.natsSubject, natsHandler)
		if err != nil {
			return fmt.Errorf("failed to subscribe to nats: %w", err)
		}
		a.sub = sub
		log.Printf("subscribed to nats subject: %s", a.config.natsSubject)

		<-ctx.Done()
		log.Println("draining nats subscription...")
		if err := a.sub.Drain(); err != nil {
			return fmt.Errorf("nats drain error: %w", err)
		}
		return nil
	})

	return g.Wait()
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down storage service...")

	a.batchWriter.Close()

	a.nc.Close()
	a.chConn.Close()

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
