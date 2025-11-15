package writer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type MetricWriter interface {
	Write(ctx context.Context, metrics []*pb.Metric) error
}

type MetricsBatchWriterConfig struct {
	BatchSize     int
	FlushInterval time.Duration
}

type MetricsBatchWriter struct {
	writer        MetricWriter
	config        MetricsBatchWriterConfig
	metricChannel chan *pb.Metric
	doneCh        chan struct{}
	wg            sync.WaitGroup
	batch         []*pb.Metric
	mutex         sync.Mutex
	flushTicker   *time.Ticker
}

func NewMetricsBatchWriter(ctx context.Context, writer MetricWriter, config MetricsBatchWriterConfig) *MetricsBatchWriter {
	bw := &MetricsBatchWriter{
		writer:        writer,
		config:        config,
		metricChannel: make(chan *pb.Metric, config.BatchSize*2),
		doneCh:        make(chan struct{}),
		batch:         make([]*pb.Metric, 0, config.BatchSize),
		flushTicker:   time.NewTicker(config.FlushInterval),
	}

	bw.wg.Add(1)
	go bw.run(ctx)
	return bw
}

func (bw *MetricsBatchWriter) AddMetric(metric *pb.Metric) error {
	select {
	case bw.metricChannel <- metric:
		return nil
	case <-bw.doneCh:
		return errors.New("metrics batch writer is closed")
	}
}

func (bw *MetricsBatchWriter) Close() {
	close(bw.doneCh)
	bw.wg.Wait()
	bw.flushTicker.Stop()
}

func (bw *MetricsBatchWriter) run(ctx context.Context) {
	defer bw.wg.Done()

	for {
		select {
		case metric := <-bw.metricChannel:
			bw.mutex.Lock()
			bw.batch = append(bw.batch, metric)
			bw.mutex.Unlock()

			if len(bw.batch) >= bw.config.BatchSize {
				bw.flush(ctx)
			}

		case <-bw.flushTicker.C:
			bw.flush(ctx)

		case <-bw.doneCh:
			close(bw.metricChannel)
			for metric := range bw.metricChannel {
				bw.batch = append(bw.batch, metric)
			}
			bw.flush(ctx)
			return
		}
	}
}

func (bw *MetricsBatchWriter) flush(ctx context.Context) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	if len(bw.batch) == 0 {
		return
	}

	log.Printf("[metrics] flushing batch of %d metrics\n", len(bw.batch))

	if err := bw.writer.Write(ctx, bw.batch); err != nil {
		log.Printf("[metrics] error writing batch: %v\n", err)
		// TODO: add retry logic/dead-letter queue
	}

	bw.batch = bw.batch[:0]
}
