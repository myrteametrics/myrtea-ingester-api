package ingester

import (
	"hash/fnv"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/google/uuid"
	config "github.com/myrteametrics/myrtea-ingester-api/v5/internals/configuration"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// In case of "live" or "hot" workers number change :
// * Stop every injections
// * Send a flush order to every worker
// * (wait until done)
// * Change workers count
// * Re-allow injections to workers

// TypedIngester is a component which process IngestRequest
// It generates UpdateCommand which are processed by the attached IndexingWorker's
type TypedIngester struct {
	Uuid                          uuid.UUID
	bulkIngester                  *BulkIngester
	DocumentType                  string
	Data                          chan *IngestRequest
	Workers                       map[int]*IndexingWorker
	maxWorkers                    int
	metricTypedIngesterQueueGauge metrics.Gauge
}

var (
	_metricTypedIngesterQueueGauge = prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		Namespace:   config.MetricNamespace,
		ConstLabels: config.MetricPrometheusLabels,
		Name:        "typedingester_queue",
		Help:        "this is the help string for typedingester_queue",
	}, []string{"typedingester"})
)

// NewTypedIngester returns a pointer to a new TypedIngester instance
func NewTypedIngester(bulkIngester *BulkIngester, documentType string) *TypedIngester {

	ingester := TypedIngester{
		Uuid:                          uuid.New(),
		bulkIngester:                  bulkIngester,
		DocumentType:                  documentType,
		Data:                          make(chan *IngestRequest, viper.GetInt("TYPEDINGESTER_QUEUE_BUFFER_SIZE")),
		Workers:                       make(map[int]*IndexingWorker),
		maxWorkers:                    viper.GetInt("INGESTER_MAXIMUM_WORKERS"),
		metricTypedIngesterQueueGauge: _metricTypedIngesterQueueGauge.With("typedingester", documentType),
	}
	_metricTypedIngesterQueueGauge.With("typedingester", documentType).Set(0)

	for i := 0; i < ingester.maxWorkers; i++ {
		worker := NewIndexingWorker(&ingester, i)
		ingester.Workers[i] = worker
		go worker.Run()
		time.Sleep(10 * time.Millisecond) // goroutine warm-up
	}
	return &ingester
}

// Run is the main routine of a TypeIngester instance
// In case of Mode == SELF
// * The in-memory cache is filled with new informations
// * An update command is send to the dedicated indexer
//
// In case of Mode == ENRICH_FROM (Which might be the same at last ?)
// * An update command is send to the dedicated indexer
//
// In case of Mode == ENRICH_TO (Which might be the same at last ?)
// * A dedicated "relation cache" is queried to find all the object which must be updated
// * One or multiple update command are sent to the dedicated indexer
//
func (ingester *TypedIngester) Run() {
	zap.L().Info("Starting TypedIngester", zap.String("documentType", ingester.DocumentType))

	for ir := range ingester.Data {
		zap.L().Debug("Receive IngestRequest", zap.String("IngesterType", ingester.DocumentType), zap.Any("IngestRequest", ir))

		workerID := getWorker(ir.Doc.ID, ingester.maxWorkers)
		worker := ingester.Workers[workerID]
		updateCommand := NewUpdateCommand(ir.Doc.Index, ir.Doc.ID, ir.DocumentType, ir.Doc, ir.MergeConfig)
		zap.L().Debug("Send UpdateCommand", zap.String("IngesterType", ingester.DocumentType), zap.Int("WorkerID", workerID), zap.Any("updateCommand", updateCommand), zap.Any("len(chan)", len(ingester.Workers[workerID].Data)))

		worker.Data <- updateCommand

		ingester.metricTypedIngesterQueueGauge.Set(float64(len(ingester.Data)))
	}
}

// getWorker returns a workerID based on a UUID hash
func getWorker(uuid string, maxWorker int) int {
	hash := hash(uuid)
	return int(hash % uint32(maxWorker))
}

// hash hash a string (for potential routing)
func hash(str string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(str))
	return h.Sum32()
}
