package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	queryhttp "github.com/vantutran2k1/aura/internal/query/http"
	"github.com/vantutran2k1/aura/pkg/auth"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	ApiPort            string `mapstructure:"api"`
	PprofPort          string `mapstructure:"pprof"`
	MetricsPort        string `mapstructure:"metrics"`
	StorageGRPCAddress string `mapstructure:"storage_grpc"`
	RedisAddress       string
}

type app struct {
	config      Config
	httpServer  *http.Server
	redisClient *redis.Client
	grpcConn    *grpc.ClientConn
	tp          *sdktrace.TracerProvider
}

func loadConfig() (*viper.Viper, Config, error) {
	v := viper.New()
	v.SetConfigFile("config.yaml")
	v.AddConfigPath(".")

	v.SetEnvPrefix("AURA")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("config.yaml not found, using defaults and env vars")
		} else {
			return nil, Config{}, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg Config
	if err := v.UnmarshalKey("query", &cfg); err != nil {
		return nil, Config{}, fmt.Errorf("failed to unmarshal query config: %w", err)
	}

	cfg.RedisAddress = v.GetString("redis")

	log.Printf("configuration loaded: %+v", cfg)
	return v, cfg, nil
}

func newApp(ctx context.Context) (*app, error) {
	v, cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	authenticator, err := auth.NewAuthenticator(v)
	if err != nil {
		return nil, fmt.Errorf("failed to init authenticator: %w", err)
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-query")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddress,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}
	log.Println("connected to redis")

	grpcConn, err := grpc.NewClient(
		cfg.StorageGRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to grpc server: %w", err)
	}
	log.Printf("connected to grpc storage service at %s", cfg.StorageGRPCAddress)
	storageClient := pb.NewStorageServiceClient(grpcConn)

	apiHandler := queryhttp.NewAPIHandler(storageClient, redisClient)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Use(authenticator.RequireAuth(auth.PermissionRead))
	r.Use(middleware.RequestID)
	r.Use(func(h http.Handler) http.Handler {
		return otelhttp.NewHandler(h, "aura-query-http")
	})

	r.Get("/v1/logs", apiHandler.HandleLogsQuery)
	r.Get("/v1/metrics", apiHandler.HandleMetricsQuery)
	r.Get("/v1/traces", apiHandler.HandleTracesQuery)

	httpServer := &http.Server{
		Addr:    cfg.ApiPort,
		Handler: r,
	}

	go pprof.StartServer(cfg.PprofPort)
	go metrics.StartMetricsServer(cfg.MetricsPort)

	return &app{
		config:      cfg,
		httpServer:  httpServer,
		redisClient: redisClient,
		grpcConn:    grpcConn,
		tp:          tp,
	}, nil
}

func (a *app) run() error {
	log.Printf("aura-query api starting on port %s", a.config.ApiPort)
	if err := a.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("http server error: %w", err)
	}
	log.Println("http server stopped")

	return nil
}

func (a *app) shutdown(ctx context.Context) error {
	log.Println("shutting down query api...")

	if err := a.httpServer.Shutdown(ctx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	if err := a.redisClient.Close(); err != nil {
		log.Printf("redis close error: %v", err)
	}

	if err := a.grpcConn.Close(); err != nil {
		log.Printf("grpc conn close error: %v", err)
	}

	if err := a.tp.Shutdown(ctx); err != nil {
		log.Printf("tracer shutdown error: %v", err)
	}

	log.Println("aura-query service shutdown gracefully")
	return nil
}
