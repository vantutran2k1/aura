package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const (
	receiverPort = "9999"
)

func webhookHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("error reading webhook body: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// TODO: this is just a simple console log handler for testing
	log.Println("--- ALERT RECEIVED ---")
	log.Printf("%s\n", string(body))
	log.Println("----------------------")

	w.WriteHeader(http.StatusOK)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/webhook-receiver", webhookHandler)

	log.Println("webhook receiver ready, listening for alerts...")

	srv := &http.Server{
		Addr:    ":" + receiverPort,
		Handler: r,
	}

	go func() {
		log.Printf("aura-webhook-receiver starting on port %s", receiverPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received, shutting down webhook receiver...")
	stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Println("webhook receiver shutdown gracefully")
}
