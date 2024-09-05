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
		return nil, errors.New("Unsupported Elasticsearch version")
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
	default:
		zap.L().Warn("mergeconfig mode not supported", zap.String("mode", command.MergeConfig.Mode.String()))
	}
	return models.Document{}
}

// // ApplyMerge execute a merge based on a specific UpdateCommand
// func ApplyMerge(doc models.Document, command UpdateCommand, secondary []models.Document) models.Document {
// 	// Important : "doc" is always the output document
// 	zap.L().Debug("ApplyMerge", zap.String("mergeMode", command.MergeConfig.Mode.String()), zap.Any("doc", doc), zap.Any("command", command), zap.Any("secondary", secondary))

// 	switch command.MergeConfig.Mode {
// 	case connector.Self:
// 		// COMMAND.NEWDOC enriched with DOC (pointer swap !) with config COMMAND.MERGECONFIG
// 		// The new pushed document become the new "reference" (and is enriched by the data of an existing one)
// 		output := command.MergeConfig.Apply(command.NewDoc, doc)
// 		zap.L().Debug("ApplyMergeResult", zap.Any("output", output))
// 		return output

// 	case connector.EnrichFrom:
// 		// COMMAND.NEWDOC enriched by SECONDARY[KEY] with config COMMAND.MERGECONFIG
// 		for _, sec := range secondary {
// 			if sec == nil {
// 				continue
// 			}
// 			key := command.MergeConfig.LinkKey
// 			source := command.NewDoc.Source.(map[string]interface{})
// 			if sec.IndexType == command.MergeConfig.Type && sec.ID == source[key] {
// 				command.MergeConfig.Apply(doc, sec)
// 				break // TODO: what about multiple external document enriching a single one ?
// 			}
// 		}
// 		zap.L().Debug("ApplyMergeResult", zap.Any("doc", doc))
// 		return doc

// 	case connector.EnrichTo:
// 		// DOC enriched WITH COMMAND.NEWDOC (NO pointer swap !) with config COMMAND.MERGECONFIG
// 		// The old existing document stay the reference (and is enriched with the data of a new one)
// 		command.MergeConfig.Apply(doc, command.NewDoc)
// 		zap.L().Debug("ApplyMergeResult", zap.Any("doc", doc))
// 		return doc
// 	}
// 	return nil
// }

// GetQuery ...
type GetQuery struct {
	DocumentType string
	ID           string
}

func (getQuery *GetQuery) convertToExecutor() models.Document {
	alias := buildAliasName(getQuery.DocumentType, index.All)
	return *models.NewDocument(getQuery.ID, alias, "document", nil)
}

func buildAliasName(documentType string, depth index.Depth) string {
	access := fmt.Sprintf("%s-%s-%s", viper.GetString("INSTANCE_NAME"), documentType, strings.ToLower(depth.String()))
	return access
}
