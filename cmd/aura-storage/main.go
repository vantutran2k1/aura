package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	storagegrpc "github.com/vantutran2k1/aura/internal/storage/grpc"
	"github.com/vantutran2k1/aura/internal/storage/writer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
)

const (
	natsAddress       = "nats://localhost:4222"
	clickhouseAddress = "localhost:9000"
	natsSubject       = "aura.processed.logs"
	batchSize         = 1000
	flushInterval     = 1 * time.Second
	grpcPort          = ":50051"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

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

	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	storageServer := storagegrpc.NewServer(chConn)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)
	reflection.Register(grpcServer)

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("gRPC server listening on %s", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				log.Printf("gRPC server error: %v", err)
			}
		}
		log.Println("gRPC server stopped")
	}()

	<-sigCh
	log.Println("Shutdown signal received, draining...")

	grpcServer.GracefulStop()

	if err := sub.Drain(); err != nil {
		log.Printf("error draining NATS subscription: %v", err)
	}
	batchWriter.Close()

	wg.Wait()

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
