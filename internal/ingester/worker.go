package ingester

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/myrteametrics/myrtea-sdk/v5/connector"
	"github.com/myrteametrics/myrtea-sdk/v5/index"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// IndexingWorker is the unit of processing which can be started in parallel for elasticsearch ingestion
type IndexingWorker interface {
	Run()
	GetMetricWorkerQueueGauge() metrics.Gauge
	GetData() chan UpdateCommand
}

func NewIndexingWorker(typedIngester *TypedIngester, id int) (IndexingWorker, error) {
	version := viper.GetInt("ELASTICSEARCH_VERSION")
	mgetBatchSize := viper.GetInt("ELASTICSEARCH_MGET_BATCH_SIZE")
	switch version {
	case 7:
		fallthrough
	case 8:
		return NewIndexingWorkerV8(typedIngester, id, mgetBatchSize), nil
	default:
		zap.L().Fatal("Unsupported Elasticsearch version", zap.Int("version", version))
		return nil, errors.New("unsupported Elasticsearch version")
	}
}

var (
	_metricWorkerQueueGauge = prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		Namespace: "myrtea",
		Name:      "worker_queue",
		Help:      "this is the help string for worker_queue",
	}, []string{"typedingester", "workerid"})
	_metricWorkerMessage = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Namespace: "myrtea",
		Name:      "worker_message_published",
		Help:      "this is the help string for worker_message_published",
	}, []string{"typedingester", "workerid", "status"})
	_metricWorkerFlushDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_flush_duration_seconds",
		Help:      "this is the help string for worker_flush_duration_seconds",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"typedingester", "workerid"})
	_metricWorkerBulkInsertDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_bulk_insert_duration_seconds",
		Help:      "this is the help string for worker_bulk_insert_duration_seconds",
		Buckets:   []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 15, 25, 45},
	}, []string{"typedingester", "workerid"})
	_metricWorkerBulkIndexDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_bulk_index_duration_seconds",
		Help:      "this is the help string for worker_bulk_index_duration_seconds",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"typedingester", "workerid"})
	_metricWorkerApplyMergesDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_apply_merges_duration_seconds",
		Help:      "this is the help string for worker_apply_merges_duration_seconds",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"typedingester", "workerid"})
	_metricWorkerDirectMultiGetDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_direct_multi_get_duration_seconds",
		Help:      "this is the help string for worker_direct_multi_get_duration_seconds",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"typedingester", "workerid"})
	_metricWorkerBulkIndexBuildBufferDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_bulk_index_build_buffer_duration_seconds",
		Help:      "this is the help string for worker_bulk_index_build_buffer_duration_seconds",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{"typedingester", "workerid"})
	_metricWorkerGetIndicesDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_get_indices_duration_seconds",
		Help:      "this is the help string for worker_get_indices_duration_seconds",
		Buckets:   []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 15, 25, 45},
	}, []string{"typedingester", "workerid"})
)

func ApplyMergeLight(doc models.Document, command UpdateCommand) models.Document {
	zap.L().Debug("ApplyMerge", zap.String("mergeMode", command.MergeConfig.Mode.String()), zap.Any("doc", doc), zap.Any("command", command))

	switch command.MergeConfig.Mode {
	case connector.Self:
		// COMMAND.NEWDOC enriched with DOC (pointer swap !) with config COMMAND.MERGECONFIG
		// The new pushed document become the new "reference" (and is enriched by the data of an existing one)
		output := command.MergeConfig.Apply(&command.NewDoc, &doc)
		zap.L().Debug("ApplyMergeResult", zap.Any("output", output))
		return *output
	case connector.EnrichTo:
		fallthrough
	case connector.EnrichFrom:
		fallthrough
	default:
		zap.L().Warn("mergeconfig mode not supported", zap.String("mode", command.MergeConfig.Mode.String()))
	}
	return models.Document{}
}

// GetQuery ...
type GetQuery struct {
	DocumentType string
	ID           string
}

func buildAliasName(documentType string, depth index.Depth) string {
	access := fmt.Sprintf("%s-%s-%s", viper.GetString("INSTANCE_NAME"), documentType, strings.ToLower(depth.String()))
	return access
}
