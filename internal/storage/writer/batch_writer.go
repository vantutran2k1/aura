package writer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type LogWriter interface {
	Write(ctx context.Context, logs []*pb.Log) error
}

type BatchWriterConfig struct {
	BatchSize     int
	FlushInterval time.Duration
}

type BatchWriter struct {
	writer      LogWriter
	config      BatchWriterConfig
	logChannel  chan *pb.Log
	doneCh      chan struct{}
	wg          sync.WaitGroup
	batch       []*pb.Log
	mutex       sync.Mutex
	flushTicker *time.Ticker
}

func NewBatchWriter(ctx context.Context, writer LogWriter, config BatchWriterConfig) *BatchWriter {
	bw := &BatchWriter{
		writer:      writer,
		config:      config,
		logChannel:  make(chan *pb.Log, config.BatchSize*2),
		doneCh:      make(chan struct{}),
		batch:       make([]*pb.Log, 0, config.BatchSize),
		flushTicker: time.NewTicker(config.FlushInterval),
	}

	bw.wg.Add(1)
	go bw.run(ctx)

	return bw
}

func (bw *BatchWriter) AddLog(log *pb.Log) error {
	select {
	case bw.logChannel <- log:
		return nil
	case <-bw.doneCh:
		return errors.New("batch writer is closed")
	}
}

func (bw *BatchWriter) Close() {
	close(bw.doneCh)
	bw.wg.Wait()
	bw.flushTicker.Stop()
}

func (bw *BatchWriter) run(ctx context.Context) {
	defer bw.wg.Done()

	for {
		select {
		case l := <-bw.logChannel:
			bw.mutex.Lock()
			bw.batch = append(bw.batch, l)
			bw.mutex.Unlock()

			if len(bw.batch) >= bw.config.BatchSize {
				bw.flush(ctx)
			}
		case <-bw.flushTicker.C:
			bw.flush(ctx)
		case <-bw.doneCh:
			close(bw.logChannel)
			for l := range bw.logChannel {
				bw.batch = append(bw.batch, l)
			}
			bw.flush(ctx)
			return
		}
	}
}

func (bw *BatchWriter) flush(ctx context.Context) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	if len(bw.batch) == 0 {
		return
	}

	log.Printf("flusing batch of %d logs\n", len(bw.batch))

	if err := bw.writer.Write(ctx, bw.batch); err != nil {
		log.Printf("error writing batch: %v\n", err)
		// TODO: add retry logic / send to a dead-letter queue
	}

	bw.batch = bw.batch[:0]
}
