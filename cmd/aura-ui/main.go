package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/viper"
)

type Config struct {
	UIPort       string `mapstructure:"port"`
	QueryService string `mapstructure:"query_service"`
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
			return Config{}, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg Config
	if err := v.UnmarshalKey("ui", &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal query config: %w", err)
	}

	log.Printf("configuration loaded: %+v", cfg)
	return cfg, nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	target, _ := url.Parse(cfg.QueryService)
	proxy := httputil.NewSingleHostReverseProxy(target)

	originalDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		originalDirector(r)
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/api")
		r.Host = target.Host
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.HandleFunc("/api/*", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("proxying request: %s", r.URL.Path)
		proxy.ServeHTTP(w, r)
	})

	fs := http.FileServer(http.Dir("./cmd/aura-ui/static"))
	r.Handle("/*", fs)

	srv := &http.Server{
		Addr:    ":" + cfg.UIPort,
		Handler: r,
	}

	go func() {
		log.Printf("aura-ui starting :%s", cfg.UIPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received.")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}
