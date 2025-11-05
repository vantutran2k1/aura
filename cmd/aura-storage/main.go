package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"github.com/vantutran2k1/aura/internal/storage/writer"
	"google.golang.org/protobuf/proto"
)

const (
	natsAddress       = "nats://localhost:4222"
	clickhouseAddress = "localhost:9000"
	natsSubject       = "aura.processed.logs"
	batchSize         = 1000
	flushInterval     = 1 * time.Second
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	chConn, err := connectClickHouse(ctx)
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse: %v", err)
	}
	defer chConn.Close()
	log.Println("connected to ClickHouse")

	chWriter := writer.NewClickHouseWriter(chConn)
	batchConfig := writer.BatchWriterConfig{
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
	}
	batchWriter := writer.NewBatchWriter(ctx, chWriter, batchConfig)
	defer batchWriter.Close()
	log.Println("batch writer started")

	nc, err := nats.Connect(natsAddress)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("connected to NATS")

	sub, err := nc.Subscribe(natsSubject, func(msg *nats.Msg) {
		var logMsg pb.Log
		if err := proto.Unmarshal(msg.Data, &logMsg); err != nil {
			log.Printf("error unmarshiling log message: %v\n", err)
			return
		}

		if err := batchWriter.AddLog(&logMsg); err != nil {
			log.Printf("error adding log to batch: %v\n", err)
		}
	})
	if err != nil {
		log.Fatalf("failed to subscribe to NATS subject: %v", err)
	}
	log.Printf("subscribed to NATS subject: %s", natsSubject)

	<-sigCh
	log.Println("Shutdown signal received, draining...")

	if err := sub.Drain(); err != nil {
		log.Printf("error draining NATS subscription: %v", err)
	}
	batchWriter.Close()

	log.Println("aura-storage service shut down gracefully")
}

func connectClickHouse(ctx context.Context) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{clickhouseAddress},
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
