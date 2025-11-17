package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	aurahttp "github.com/vantutran2k1/aura/internal/ingestor/http"
	"github.com/vantutran2k1/aura/pkg/metrics"
	"github.com/vantutran2k1/aura/pkg/pprof"
	"github.com/vantutran2k1/aura/pkg/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Config struct {
	APIPort            string `mapstructure:"api"`
	PprofPort          string `mapstructure:"pprof"`
	MetricsPort        string `mapstructure:"metrics"`
	NatsAddress        string `mapstructure:"nats"`
	NatsLogsSubject    string `mapstructure:"natsLogsSubject"`
	NatsMetricsSubject string `mapstructure:"natsMetricsSubject"`
	NatsTracesSubject  string `mapstructure:"natsTracesSubject"`
}

type app struct {
	config     Config
	nc         *nats.Conn
	httpServer *http.Server
	tp         *sdktrace.TracerProvider
}

func loadConfig() (Config, error) {
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
			return Config{}, fmt.Errorf("faiiled to read config: %w", err)
		}
	}

	var cfg Config
	if err := v.UnmarshalKey("ingestor", &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal ingestor config: %w", err)
	}

	cfg.NatsAddress = v.GetString("nats")
	cfg.NatsLogsSubject = v.GetString("subjects.logs.raw")
	cfg.NatsMetricsSubject = v.GetString("subjects.metrics.raw")
	cfg.NatsTracesSubject = v.GetString("subjects.traces.raw")

	log.Printf("configuration loaded: %+v", cfg)
	return cfg, nil
}

func newApp(ctx context.Context) (*app, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	tp, err := tracing.InitTracerProvider(ctx, "aura-ingestor")
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}

	nc, err := nats.Connect(cfg.NatsAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("connected to nats")

	apiHandler := aurahttp.NewAPIHandler(
		nc,
		cfg.NatsLogsSubject,
		cfg.NatsMetricsSubject,
		cfg.NatsTracesSubject,
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
		Addr:    cfg.APIPort,
		Handler: r,
	}

	go pprof.StartServer(cfg.PprofPort)
	go metrics.StartMetricsServer(cfg.MetricsPort)

	return &app{
		config:     cfg,
		nc:         nc,
		httpServer: httpServer,
		tp:         tp,
	}, nil
}

func (a *app) run() error {
	log.Printf("aura-ingestor starting on port %s", a.config.APIPort)
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
