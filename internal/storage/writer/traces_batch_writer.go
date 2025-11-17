package writer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type TraceWriter interface {
	Write(ctx context.Context, spans []*pb.Span) error
}

type TracesBatchWriterConfig struct {
	BatchSize     int
	FlushInterval time.Duration
}

type TracesBatchWriter struct {
	writer      TraceWriter
	config      TracesBatchWriterConfig
	spanChannel chan *pb.Span
	doneCh      chan struct{}
	wg          sync.WaitGroup
	batch       []*pb.Span
	mutex       sync.Mutex
	flushTicker *time.Ticker
}

func NewTracesBatchWriter(ctx context.Context, writer TraceWriter, config TracesBatchWriterConfig) *TracesBatchWriter {
	bw := &TracesBatchWriter{
		writer:      writer,
		config:      config,
		spanChannel: make(chan *pb.Span, config.BatchSize*2),
		doneCh:      make(chan struct{}),
		batch:       make([]*pb.Span, 0, config.BatchSize),
		flushTicker: time.NewTicker(config.FlushInterval),
	}

	bw.wg.Add(1)
	go bw.run(ctx)
	return bw
}

func (bw *TracesBatchWriter) AddSpan(span *pb.Span) error {
	select {
	case bw.spanChannel <- span:
		return nil
	case <-bw.doneCh:
		return errors.New("traces batch writer is closed")
	}
}

func (bw *TracesBatchWriter) Close() {
	close(bw.doneCh)
	bw.wg.Wait()
	bw.flushTicker.Stop()
}

func (bw *TracesBatchWriter) run(ctx context.Context) {
	defer bw.wg.Done()

	for {
		select {
		case span := <-bw.spanChannel:
			bw.mutex.Lock()
			bw.batch = append(bw.batch, span)
			bw.mutex.Unlock()

			if len(bw.batch) >= bw.config.BatchSize {
				bw.flush(ctx)
			}

		case <-bw.flushTicker.C:
			bw.flush(ctx)

		case <-bw.doneCh:
			close(bw.spanChannel)
			for span := range bw.spanChannel {
				bw.batch = append(bw.batch, span)
			}
			bw.flush(ctx)
			return
		}
	}
}

func (bw *TracesBatchWriter) flush(ctx context.Context) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	if len(bw.batch) == 0 {
		return
	}

	log.Printf("[traces] flushing batch of %d spans\n", len(bw.batch))

	if err := bw.writer.Write(ctx, bw.batch); err != nil {
		log.Printf("[traces] error writing batch: %v\n", err)
	}

	bw.batch = bw.batch[:0]
}
