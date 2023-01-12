package ingester

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/hashicorp/go-retryablehttp"
	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/merge"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v4/index"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
	"github.com/olivere/elastic"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// IndexingWorker is the unit of processing which can be started in parallel for elasticsearch ingestion
type IndexingWorker struct {
	TypedIngester             *TypedIngester
	ID                        int
	Data                      chan *UpdateCommand
	Client                    *elasticsearch.EsExecutor
	metricWorkerQueueGauge    metrics.Gauge
	metricWorkerMessage       metrics.Counter
	metricWorkerFlushDuration metrics.Histogram
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
)

// NewIndexingWorker returns a new IndexingWorker
func NewIndexingWorker(typedIngester *TypedIngester, id int) *IndexingWorker {

	var data chan *UpdateCommand
	if workerQueueSize := viper.GetInt("WORKER_QUEUE_BUFFER_SIZE"); workerQueueSize > 0 {
		data = make(chan *UpdateCommand, viper.GetInt("WORKER_QUEUE_BUFFER_SIZE"))
	} else {
		data = make(chan *UpdateCommand)
	}

	zap.L().Info("Initialize Elasticsearch client", zap.String("status", "in_progress"))
	// client, err := elasticsearch.NewEsExecutor(context.Background(), viper.GetStringSlice("ELASTICSEARCH_URLS"))
	// if err != nil {
	// 	zap.L().Error("Elasticsearch client initialization", zap.Error(err))
	// } else {
	// 	zap.L().Info("Initialize Elasticsearch client", zap.String("status", "done"))
	// }

	var zapDebugConfig zap.Config
	if viper.GetBool("LOGGER_PRODUCTION") {
		zapDebugConfig = zap.NewProductionConfig()
	} else {
		zapDebugConfig = zap.NewDevelopmentConfig()
	}
	zapDebugConfig.Level.SetLevel(zap.DebugLevel)
	zapDebugLogger, err := zapDebugConfig.Build(zap.AddStacktrace(zap.PanicLevel))
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer zapDebugLogger.Sync()

	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient.Timeout = viper.GetDuration("SINK_HTTP_TIMEOUT")
	retryClient.Logger = utils.NewZapLeveledLogger(zapDebugLogger)

	client, err := elastic.NewClient(elastic.SetSniff(false),
		elastic.SetHealthcheckTimeoutStartup(60*time.Second),
		elastic.SetURL(viper.GetStringSlice("ELASTICSEARCH_URLS")...),
		elastic.SetHttpClient(retryClient.StandardClient()),
	)
	if err != nil {
		zap.L().Error("Elasticsearch client initialization", zap.Error(err))
	} else {
		zap.L().Info("Initialize Elasticsearch client", zap.String("status", "done"))
	}
	executor := &elasticsearch.EsExecutor{Client: client}

	worker := &IndexingWorker{
		TypedIngester:             typedIngester,
		ID:                        id,
		Data:                      data,
		Client:                    executor,
		metricWorkerQueueGauge:    _metricWorkerQueueGauge.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerMessage:       _metricWorkerMessage.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerFlushDuration: _metricWorkerFlushDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
	}
	worker.metricWorkerQueueGauge.Set(0)
	worker.metricWorkerMessage.With("status", "flushed").Add(0)

	return worker
}

// Run start a worker
func (worker *IndexingWorker) Run() {
	zap.L().Info("Starting IndexingWorker",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)

	// Throttle testing only
	// for {
	// 	select {
	// 	case uc := <-worker.Data:
	// 		// zap.L().Info("Receive UpdateCommand", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
	// 		_ = uc
	// 		worker.metricWorkerQueueGauge.Set(float64(len(worker.Data)))
	// 		time.Sleep(time.Millisecond * 1000)
	// 	}
	// }

	bufferLength := viper.GetInt("WORKER_MAXIMUM_BUFFER_SIZE")
	buffer := make([]*UpdateCommand, 0)

	forceFlushTimeout := viper.GetInt("WORKER_FORCE_FLUSH_TIMEOUT_SEC")
	forceFlush := worker.resetForceFlush(forceFlushTimeout)

	for {
		select {

		// Send indexing bulk (when buffer is full or on timeout)
		case <-forceFlush:
			zap.L().Info("Try on after timeout reached", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout))
			if len(buffer) > 0 {
				zap.L().Info("Flushing on timeout reached", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
			}
			forceFlush = worker.resetForceFlush(forceFlushTimeout)

		// Build indexing bulk
		case uc := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
			buffer = append(buffer, uc)
			if len(buffer) >= bufferLength {
				zap.L().Info("Try flushing on full buffer", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
				forceFlush = worker.resetForceFlush(forceFlushTimeout)
			}
			worker.metricWorkerQueueGauge.Set(float64(len(worker.Data)))
		}
	}
}

func (worker *IndexingWorker) resetForceFlush(sec int) <-chan time.Time {
	return time.After(time.Duration(sec) * time.Second)
}

func (worker *IndexingWorker) flushEsBuffer(buffer []*UpdateCommand) {
	if len(buffer) == 0 {
		return
	}

	start := time.Now()

	m := make(map[string][]*UpdateCommand)
	for _, uc := range buffer {
		if m[uc.DocumentID] != nil {
			m[uc.DocumentID] = append(m[uc.DocumentID], uc)
		} else {
			m[uc.DocumentID] = []*UpdateCommand{uc}
		}
	}

	sl := make([][]*UpdateCommand, 0)
	for _, v := range m {
		sl = append(sl, v)
	}

	for key, entry := range m {
		zap.L().Debug("flushEsBuffer", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID), zap.String("key", key), zap.Int("len(entry)", len(entry)))
	}

	if viper.GetBool("DEBUG_DRY_RUN_ELASTICSEARCH") {
		return
	}
	worker.BulkChainedUpdate(sl)

	worker.metricWorkerMessage.With("status", "flushed").Add(float64(len(buffer)))
	worker.metricWorkerFlushDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
}

// BulkChainedUpdate process multiple groups of UpdateCommand
// It execute sequentialy every single UpdateCommand on a specific "source" document, for each group of commands
func (worker *IndexingWorker) BulkChainedUpdate(documents [][]*UpdateCommand) {

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))

	docs := make([]GetQuery, 0)
	for _, commands := range documents {
		docs = append(docs, GetQuery{DocumentType: commands[0].DocumentType, ID: commands[0].DocumentID})
	}
	if len(docs) == 0 {
		zap.L().Warn("empty docs update", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))
		return
	}

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "getindices"))

	indices, err := worker.getIndices(docs[0].DocumentType)
	if err != nil {
		zap.L().Error("getIndices", zap.Error(err))
	}
	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "multiGetFindRefDocsFull"))

	refDocs, err := worker.multiGetFindRefDocsFull(indices, docs)
	if err != nil {
		zap.L().Error("multiGetFindRefDocsFull", zap.Error(err))
	}
	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))

	push, err := worker.applyMerges(documents, refDocs)
	if err != nil {
		zap.L().Error("applyMerges", zap.Error(err))
	}
	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))

	err = worker.bulkIndex(push)
	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}
	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

func (worker *IndexingWorker) getIndices(documentType string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	alias := buildAliasName(documentType, index.Patch)
	indices, err := worker.Client.GetIndicesByAlias(ctx, alias)
	if err != nil {
		zap.L().Error("GetIndicesByAlias", zap.Error(err), zap.String("alias", alias))
	}
	return indices, err
}

func (worker *IndexingWorker) multiGetFindRefDocsFull(indices []string, docs []GetQuery) ([]*models.Document, error) {
	// TODO: parrallelism of multiple bulk get ?
	// Or chain GET on missing results only (instead of full set)
	zap.L().Info("multiGetFindRefDocsFull", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "loop indices"))
	refDocs := make([]*models.Document, 0)
	for _, index := range indices {

		zap.L().Info("multiGetFindRefDocsFull", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("step", "index"))
		responseDocs, err := worker.multiGetFindRefDocs(index, docs)
		if err != nil {
			zap.L().Error("multiGetFindRefDocs", zap.Error(err))
		}
		zap.L().Info("multiGetFindRefDocsFull", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("step", "loop docs"))

		for i, d := range responseDocs {
			data, err := jsoniter.Marshal(d.Source)
			if err != nil {
				zap.L().Error("update multiget unmarshal", zap.Error(err))
			}

			var source map[string]interface{}
			err = jsoniter.Unmarshal(data, &source)
			if err != nil {
				zap.L().Error("update multiget unmarshal", zap.Error(err))
			}

			if len(refDocs) > i && refDocs[i] == nil {
				if d.Found {
					refDocs[i] = models.NewDocument(d.Id, d.Index, d.Type, source)
				}
			} else {
				if d.Found {
					refDocs = append(refDocs, models.NewDocument(d.Id, d.Index, d.Type, source))
				} else {
					refDocs = append(refDocs, nil)
				}
			}
		}
	}
	return refDocs, nil
}

func (worker *IndexingWorker) multiGetFindRefDocs(index string, queries []GetQuery) ([]*elastic.GetResult, error) {
	d := make([]*models.Document, 0)
	for _, doc := range queries {
		d = append(d, models.NewDocument(doc.ID, index, "document", nil))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	response, err := worker.Client.MultiGet(ctx, d)
	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}
	return response.Docs, nil
}

func (worker *IndexingWorker) applyMerges(documents [][]*UpdateCommand, refDocs []*models.Document) ([]*models.Document, error) {
	var push = make([]*models.Document, 0)
	var i int
	for _, commands := range documents {
		var doc *models.Document
		if len(refDocs) > i {
			doc = refDocs[i]
		}

		// Index setup should probably not be here (be before in the indexing chain)
		for _, command := range commands {
			if command.NewDoc.Index == "" {
				command.NewDoc.Index = buildAliasName(command.DocumentType, index.Last)
			}
			doc = ApplyMergeLight(doc, command)
		}
		doc.IndexType = "document"
		push = append(push, doc)
		i++ // synchronise map iteration with reponse.Docs
	}
	return push, nil
}

func (worker *IndexingWorker) bulkIndex(docs []*models.Document) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	bulkResponse, err := worker.Client.BulkIndex(ctx, docs)
	if err != nil {
		zap.L().Error("BulkIndex response", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Error(err))
		return err
	}
	if bulkResponse != nil && len(bulkResponse.Failed()) > 0 {
		zap.L().Error("Error during bulkIndex", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Any("bulkResponse.Failed()", bulkResponse.Failed()))
		return errors.New("bulkindex failed")
	}
	return nil
}

func ApplyMergeLight(doc *models.Document, command *UpdateCommand) *models.Document {
	zap.L().Debug("ApplyMerge", zap.String("mergeMode", command.MergeConfig.Mode.String()), zap.Any("doc", doc), zap.Any("command", command))

	switch command.MergeConfig.Mode {
	case merge.Self:
		// COMMAND.NEWDOC enriched with DOC (pointer swap !) with config COMMAND.MERGECONFIG
		// The new pushed document become the new "reference" (and is enriched by the data of an existing one)
		output := command.MergeConfig.Apply(command.NewDoc, doc)
		zap.L().Debug("ApplyMergeResult", zap.Any("output", output))
		return output
	default:
		zap.L().Warn("mergeconfig mode not supported", zap.String("mode", command.MergeConfig.Mode.String()))
	}
	return nil
}

// // ApplyMerge execute a merge based on a specific UpdateCommand
// func ApplyMerge(doc *models.Document, command *UpdateCommand, secondary []*models.Document) *models.Document {
// 	// Important : "doc" is always the output document
// 	zap.L().Debug("ApplyMerge", zap.String("mergeMode", command.MergeConfig.Mode.String()), zap.Any("doc", doc), zap.Any("command", command), zap.Any("secondary", secondary))

// 	switch command.MergeConfig.Mode {
// 	case merge.Self:
// 		// COMMAND.NEWDOC enriched with DOC (pointer swap !) with config COMMAND.MERGECONFIG
// 		// The new pushed document become the new "reference" (and is enriched by the data of an existing one)
// 		output := command.MergeConfig.Apply(command.NewDoc, doc)
// 		zap.L().Debug("ApplyMergeResult", zap.Any("output", output))
// 		return output

// 	case merge.EnrichFrom:
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

// 	case merge.EnrichTo:
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

func (getQuery *GetQuery) convertToExecutor() *models.Document {
	alias := buildAliasName(getQuery.DocumentType, index.All)
	return models.NewDocument(getQuery.ID, alias, "document", nil)
}

func buildAliasName(documentType string, depth index.Depth) string {
	access := fmt.Sprintf("%s-%s-%s", viper.GetString("INSTANCE_NAME"), documentType, strings.ToLower(depth.String()))
	return access
}
