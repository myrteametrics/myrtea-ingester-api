package ingester

import (
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

// IndexingWorker is the common interface implemented by every worker variant.
type IndexingWorker interface {
	Run()
	GetMetricWorkerQueueGauge() metrics.Gauge
	GetData() chan UpdateCommand
}

// NewIndexingWorker constructs the concrete worker implementation selected at
// compile time (currently always IndexingWorkerV8).
func NewIndexingWorker(typedIngester *TypedIngester, id int) (IndexingWorker, error) {
	mgetBatchSize := viper.GetInt("ELASTICSEARCH_MGET_BATCH_SIZE")
	return NewIndexingWorkerV8(typedIngester, id, mgetBatchSize), nil
}

const moduleName = "typedingester"

// ─── Prometheus metric descriptors ────────────────────────────────────────────
// All metrics are label-free at declaration time; labels are added per-worker
// via With("typedingester", <docType>, "workerid", <id>).

var (
	_metricWorkerQueueGauge = prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		Namespace: "myrtea",
		Name:      "worker_queue",
		Help:      "Number of UpdateCommands currently waiting in the worker's inbound channel.",
	}, []string{moduleName, "workerid"})

	_metricWorkerMessage = prometheus.NewCounterFrom(stdprometheus.CounterOpts{
		Namespace: "myrtea",
		Name:      "worker_message_published",
		Help:      "Total number of UpdateCommands flushed to Elasticsearch, partitioned by status.",
	}, []string{moduleName, "workerid", "status"})

	_metricWorkerFlushDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_flush_duration_seconds",
		Help:      "End-to-end duration of one flushEsBuffer call (mget + merge + bulk write), in seconds.",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{moduleName, "workerid"})

	_metricWorkerBulkInsertDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_bulk_insert_duration_seconds",
		Help:      "Duration of the raw HTTP bulk request to Elasticsearch, in seconds.",
		Buckets:   []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 15, 25, 45},
	}, []string{moduleName, "workerid"})

	_metricWorkerBulkIndexDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_bulk_index_duration_seconds",
		Help:      "Duration of the bulkIndex / bulkCreate step (includes HTTP + response parsing), in seconds.",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{moduleName, "workerid"})

	_metricWorkerApplyMergesDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_apply_merges_duration_seconds",
		Help:      "Duration of the in-memory merge step (applyMerges / applyDirectMerges), in seconds.",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{moduleName, "workerid"})

	_metricWorkerDirectMultiGetDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_direct_multi_get_duration_seconds",
		Help:      "Duration of the mget lookup step (directMultiGetDocs or multiGetFindRefDocsFullV2), in seconds.",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{moduleName, "workerid"})

	_metricWorkerBulkIndexBuildBufferDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_bulk_index_build_buffer_duration_seconds",
		Help:      "Duration of the NDJSON payload serialisation step before sending the bulk request, in seconds.",
		Buckets:   []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
	}, []string{moduleName, "workerid"})

	_metricWorkerGetIndicesDuration = prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "worker_get_indices_duration_seconds",
		Help:      "Duration of the alias-resolution step (getIndices), in seconds. Only applicable when ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false.",
		Buckets:   []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 15, 25, 45},
	}, []string{moduleName, "workerid"})
)

// ─── Merge logic ───────────────────────────────────────────────────────────────

// ApplyMergeLight applies the merge strategy defined in command.MergeConfig to
// produce an updated document.
//
// In connector.Self mode (the only one currently supported) the incoming
// document (command.NewDoc) becomes the new source of truth and is enriched
// with fields from the previously-stored document (doc).  The merge config
// controls exactly which fields are copied or overwritten.
//
// Unsupported modes (EnrichTo, EnrichFrom) log a warning and return an empty
// document to avoid silently persisting garbage data.
func ApplyMergeLight(doc models.Document, command UpdateCommand) models.Document {
	zap.L().Debug("ApplyMergeLight",
		zap.String("mergeMode", command.MergeConfig.Mode.String()),
		zap.Any("existingDoc", doc),
		zap.Any("command", command),
	)

	switch command.MergeConfig.Mode {
	case connector.Self:
		// command.NewDoc is enriched with fields from doc according to MergeConfig.
		// The pointer swap means the new document becomes the authoritative state.
		output := command.MergeConfig.Apply(&command.NewDoc, &doc)
		zap.L().Debug("ApplyMergeLight result", zap.Any("output", output))
		return *output

	case connector.EnrichTo:
		fallthrough
	case connector.EnrichFrom:
		fallthrough
	default:
		zap.L().Warn("ApplyMergeLight: unsupported merge mode",
			zap.String("mode", command.MergeConfig.Mode.String()),
		)
	}
	return models.Document{}
}

// ─── Shared helpers ────────────────────────────────────────────────────────────

// GetQuery holds the minimum information needed to look up a document in ES.
type GetQuery struct {
	DocumentType string
	ID           string
}

// buildAliasName constructs the Elasticsearch alias name for a given document
// type and index depth.  The alias follows the convention:
//
//	<INSTANCE_NAME>-<documentType>-<depth>
//
// For example: "prod-order-last" or "prod-order-patch".
func buildAliasName(documentType string, depth index.Depth) string {
	return fmt.Sprintf("%s-%s-%s",
		viper.GetString("INSTANCE_NAME"),
		documentType,
		strings.ToLower(depth.String()),
	)
}
