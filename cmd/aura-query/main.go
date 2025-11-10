package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	queryhttp "github.com/vantutran2k1/aura/internal/query/http"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	storageGRPCAddress = "localhost:50051"
	redisAddress       = "localhost:6379"
	apiPort            = "8081"
	pprofPort          = "6061"
	metricsPort        = "9092"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tp, err := tracing.InitTracerProvider(ctx, "aura-query")
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("error shutting down tracer provider: %v", err)
		}
	}()

	go pprof.StartServer("localhost:" + pprofPort)

	go metrics.StartMetricsServer("localhost:" + metricsPort)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()
	log.Println("connected to Redis")

	conn, err := grpc.NewClient(
		storageGRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	storageClient := pb.NewStorageServiceClient(conn)
	log.Printf("connected to gRPC storage service at %s", storageGRPCAddress)

	apiHandler := queryhttp.NewAPIHandler(storageClient, redisClient)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)
	r.Use(func(h http.Handler) http.Handler {
		return otelhttp.NewHandler(h, "aura-query-http")
	})

	r.Get("/v1/logs", apiHandler.HandleLogsQuery)

	srv := &http.Server{
		Addr:    ":" + apiPort,
		Handler: r,
	}

	go func() {
		log.Printf("aura-query API starting on port %s", apiPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received, shutting down query API")
	stop()

	shutDownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutDownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Println("aura-query service shut down gracefully")
}
