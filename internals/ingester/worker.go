package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	goelasticsearch "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/merge"
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
	Uuid                      uuid.UUID
	TypedIngester             *TypedIngester
	ID                        int
	Data                      chan UpdateCommand
	Client                    *goelasticsearch.Client
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

	var data chan UpdateCommand
	if workerQueueSize := viper.GetInt("WORKER_QUEUE_BUFFER_SIZE"); workerQueueSize > 0 {
		data = make(chan UpdateCommand, workerQueueSize)
	} else {
		data = make(chan UpdateCommand)
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
	retryClient.HTTPClient.Timeout = viper.GetDuration("ELASTICSEARCH_HTTP_TIMEOUT")
	retryClient.Logger = utils.NewZapLeveledLogger(zapDebugLogger)

	cfg := goelasticsearch.Config{
		Addresses:     viper.GetStringSlice("ELASTICSEARCH_URLS"),
		EnableMetrics: true,
		MaxRetries:    5,
		RetryOnStatus: []int{502, 503, 504, 429},
	}
	goelasticsearch.NewDefaultClient()
	client, err := goelasticsearch.NewClient(cfg)
	if err != nil {
		zap.L().Error("GoElasticsearch client initialization", zap.Error(err))
	} else {
		zap.L().Info("Initialize GoElasticsearch client", zap.String("status", "done"), zap.Duration("http timeout", viper.GetDuration("ELASTICSEARCH_HTTP_TIMEOUT")))
	}

	// client, err := elastic.NewClient(elastic.SetSniff(false),
	// 	elastic.SetHealthcheckTimeoutStartup(60*time.Second),
	// 	elastic.SetURL(viper.GetStringSlice("ELASTICSEARCH_URLS")...),
	// 	elastic.SetHttpClient(retryClient.StandardClient()),
	// )
	// if err != nil {
	// 	zap.L().Error("Elasticsearch client initialization", zap.Error(err))
	// } else {
	// 	zap.L().Info("Initialize Elasticsearch client", zap.String("status", "done"), zap.Duration("http timeout", viper.GetDuration("ELASTICSEARCH_HTTP_TIMEOUT")))
	// }
	// executor := &elasticsearch.EsExecutor{Client: client}

	worker := &IndexingWorker{
		Uuid:          uuid.New(),
		TypedIngester: typedIngester,
		ID:            id,
		Data:          data,
		// Client:                    executor,
		Client:                    client,
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
	// 		// zap.L().Info("Receive UpdateCommand", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
	// 		_ = uc
	// 		worker.metricWorkerQueueGauge.Set(float64(len(worker.Data)))
	// 		time.Sleep(time.Millisecond * 1000)
	// 	}
	// }

	bufferLength := viper.GetInt("WORKER_MAXIMUM_BUFFER_SIZE")
	buffer := make([]UpdateCommand, 0)

	forceFlushTimeout := viper.GetInt("WORKER_FORCE_FLUSH_TIMEOUT_SEC")
	forceFlush := worker.resetForceFlush(forceFlushTimeout)

	for {
		select {

		// Send indexing bulk (when buffer is full or on timeout)
		case <-forceFlush:
			zap.L().Info("Try on after timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout))
			if len(buffer) > 0 {
				zap.L().Info("Flushing on timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
			}
			forceFlush = worker.resetForceFlush(forceFlushTimeout)

		// Build indexing bulk
		case uc := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
			buffer = append(buffer, uc)
			if len(buffer) >= bufferLength {
				zap.L().Info("Try flushing on full buffer", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)))
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

func (worker *IndexingWorker) flushEsBuffer(buffer []UpdateCommand) {
	if len(buffer) == 0 {
		return
	}

	start := time.Now()
	updateCommandGroupsMap := make(map[string][]UpdateCommand)
	for _, uc := range buffer {
		if updateCommandGroupsMap[uc.DocumentID] != nil {
			updateCommandGroupsMap[uc.DocumentID] = append(updateCommandGroupsMap[uc.DocumentID], uc)
		} else {
			updateCommandGroupsMap[uc.DocumentID] = []UpdateCommand{uc}
		}
	}

	updateCommandGroups := make([][]UpdateCommand, 0)
	for _, v := range updateCommandGroupsMap {
		updateCommandGroups = append(updateCommandGroups, v)
	}

	if viper.GetBool("DEBUG_DRY_RUN_ELASTICSEARCH") {
		return
	}
	if viper.GetBool("ELASTICSEARCH_DIRECT_MULTI_GET_MODE") {
		worker.directBulkChainedUpdate(updateCommandGroups)
	} else {
		worker.bulkChainedUpdate(updateCommandGroups)
	}

	worker.metricWorkerMessage.With("status", "flushed").Add(float64(len(buffer)))
	worker.metricWorkerFlushDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
}

func (worker *IndexingWorker) directBulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {
	zap.L().Info("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))

	zap.L().Info("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "directMultiGetDocs"))
	refDocs, err := worker.directMultiGetDocs(updateCommandGroups)
	if err != nil {
		zap.L().Error("directMultiGetDocs", zap.Error(err))
	}

	zap.L().Info("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))
	push, err := worker.applyMergesV2(updateCommandGroups, refDocs)
	if err != nil {
		zap.L().Error("applyMergesV2", zap.Error(err))
	}

	zap.L().Info("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))
	err = worker.bulkIndex(push)
	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}

	zap.L().Info("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

func (worker *IndexingWorker) directMultiGetDocs(updateCommandGroups [][]UpdateCommand) ([]models.Document, error) {
	docs := make([]*models.Document, 0)
	for _, updateCommandGroup := range updateCommandGroups {
		docs = append(docs, &models.Document{Index: updateCommandGroup[0].Index, ID: updateCommandGroup[0].DocumentID})
	}

	zap.L().Info("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))

	source := make(map[string]interface{})
	sourceItems := make([]interface{}, len(docs))
	for i, doc := range docs {
		src, err := elastic.NewMultiGetItem().Index(doc.Index).Type(doc.IndexType).Id(doc.ID).Source()
		if err != nil {
			zap.L().Warn("cannot convert item to source()", zap.Error(err))
			continue
		}
		sourceItems[i] = src
	}
	source["docs"] = sourceItems

	var body = new(bytes.Buffer)
	err := json.NewEncoder(body).Encode(source)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
		// return make([]*elastic.GetResult, 0), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := esapi.MgetRequest{
		Body: body,
	}.Do(ctx, worker.Client)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
		// return make([]*elastic.GetResult, 0), err
	}
	defer res.Body.Close()
	if res.IsError() {
		zap.L().Error("mgetRequest failed", zap.Error(err))
		// return make([]*elastic.GetResult, 0), err
	}

	var response elastic.MgetResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		zap.L().Error("parsing the response body", zap.Error(err))
		// return make([]*elastic.GetResult, 0), err
	}
	zap.L().Info("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		// return make([]*elastic.GetResult, 0), err
	}

	refDocs := make([]models.Document, 0)
	for i, d := range response.Docs {
		data, err := jsoniter.Marshal(d.Source)
		if err != nil {
			zap.L().Error("update multiget unmarshal", zap.Error(err))
		}

		var source map[string]interface{}
		err = jsoniter.Unmarshal(data, &source)
		if err != nil {
			zap.L().Error("update multiget unmarshal", zap.Error(err))
		}

		if len(refDocs) > i && refDocs[i].ID == "" {
			if d.Found {
				refDocs[i] = models.Document{ID: d.Id, Index: d.Index, IndexType: d.Type, Source: source}
			}
		} else {
			if d.Found {
				refDocs = append(refDocs, models.Document{ID: d.Id, Index: d.Index, IndexType: d.Type, Source: source})
			} else {
				refDocs = append(refDocs, models.Document{})
			}
		}
	}

	return refDocs, nil
}

// bulkChainedUpdate process multiple groups of UpdateCommand
// It execute sequentialy every single UpdateCommand on a specific "source" document, for each group of commands
func (worker *IndexingWorker) bulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))
	docs := make([]GetQuery, 0)
	for _, commands := range updateCommandGroups {
		docs = append(docs, GetQuery{DocumentType: commands[0].DocumentType, ID: commands[0].DocumentID})
	}
	if len(docs) == 0 {
		zap.L().Warn("empty docs update", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))
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

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))
	push, err := worker.applyMerges(updateCommandGroups, refDocs)
	if err != nil {
		zap.L().Error("applyMerges", zap.Error(err))
	}

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))
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

	res, err := esapi.IndicesGetAliasRequest{Name: []string{alias}}.Do(ctx, worker.Client)
	if err != nil {
		zap.L().Error("", zap.Error(err))
		return make([]string, 0), errors.New("alias not found")

	}
	defer res.Body.Close()
	if res.IsError() {
		zap.L().Error("Alias cannot be found", zap.String("alias", alias))
		return make([]string, 0), errors.New("alias not found")
	}

	r := make(map[string]struct {
		Aliases map[string]interface{} `json:"aliases"`
	})
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode alias response", zap.Error(err), zap.String("alias", alias), zap.Any("request", esapi.IndicesGetAliasRequest{Name: []string{alias}}))
		return make([]string, 0), errors.New("alias not found")
	}

	indices := make([]string, 0)
	for k := range r {
		indices = append(indices, k)
	}
	return indices, err
}

func (worker *IndexingWorker) multiGetFindRefDocsFull(indices []string, docs []GetQuery) ([]models.Document, error) {
	// TODO: parrallelism of multiple bulk get ?
	// Or chain GET on missing results only (instead of full set)
	// zap.L().Info("multiGetFindRefDocsFull", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "loop indices"))
	refDocs := make([]models.Document, 0)
	for _, index := range indices {

		// zap.L().Info("multiGetFindRefDocsFull", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("step", "index"))
		responseDocs, err := worker.multiGetFindRefDocs(index, docs)
		if err != nil {
			zap.L().Error("multiGetFindRefDocs", zap.Error(err))
		}
		// zap.L().Info("multiGetFindRefDocsFull", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("step", "loop docs"))

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

			if len(refDocs) > i && refDocs[i].ID == "" {
				if d.Found {
					refDocs[i] = *models.NewDocument(d.Id, d.Index, d.Type, source)
				}
			} else {
				if d.Found {
					refDocs = append(refDocs, *models.NewDocument(d.Id, d.Index, d.Type, source))
				} else {
					refDocs = append(refDocs, models.Document{})
				}
			}
		}
	}
	return refDocs, nil
}

func (worker *IndexingWorker) multiGetFindRefDocs(index string, queries []GetQuery) ([]*elastic.GetResult, error) {
	if len(queries) == 0 {
		return nil, errors.New("docs[] is empty")
	}

	zap.L().Info("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index))

	source := make(map[string]interface{})
	sourceItems := make([]interface{}, len(queries))
	for i, query := range queries {
		src, err := elastic.NewMultiGetItem().Type("document").Id(query.ID).Source()
		if err != nil {
			zap.L().Warn("cannot convert item to source()", zap.Error(err))
			continue
		}
		sourceItems[i] = src
	}
	source["docs"] = sourceItems

	var body = new(bytes.Buffer)
	err := json.NewEncoder(body).Encode(source)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := esapi.MgetRequest{
		DocumentType: "document",
		Index:        index,
		Body:         body,
	}.Do(ctx, worker.Client)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}
	defer res.Body.Close()
	if res.IsError() {
		zap.L().Error("mgetRequest failed", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}

	var response elastic.MgetResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		zap.L().Error("parsing the response body", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}
	zap.L().Info("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}
	return response.Docs, nil
}

func (worker *IndexingWorker) applyMerges(documents [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {
	var push = make([]models.Document, 0)
	var i int
	for _, commands := range documents {
		var doc models.Document
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

func (worker *IndexingWorker) applyMergesV2(updateCommandGroups [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {

	push := make([]models.Document, 0)
	for i, updateCommandGroup := range updateCommandGroups {
		var pushDoc models.Document
		if len(refDocs) > i {
			pushDoc = models.Document{ID: refDocs[i].ID, Index: refDocs[i].Index, IndexType: refDocs[i].IndexType, Source: refDocs[i].Source}
		}
		for _, command := range updateCommandGroup {
			if pushDoc.ID == "" {
				pushDoc = models.Document{ID: command.NewDoc.ID, Index: command.NewDoc.Index, IndexType: command.NewDoc.IndexType, Source: command.NewDoc.Source}
			} else {
				pushDoc = ApplyMergeLight(pushDoc, command)
			}
		}
		push = append(push, pushDoc)
	}

	return push, nil
}

func (worker *IndexingWorker) bulkIndex(docs []models.Document) error {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buf := bytes.NewBuffer(make([]byte, 0))

	for _, doc := range docs {
		req := elastic.NewBulkIndexRequest().Index(doc.Index).Type(doc.IndexType).Id(doc.ID).Doc(doc.Source)
		source, err := req.Source()
		if err != nil {
			zap.L().Error("", zap.Error(err))
		}
		for _, line := range source {
			buf.WriteString(line)
			buf.WriteByte('\n')
		}
	}
	res, err := esapi.BulkRequest{Body: buf}.Do(ctx, worker.Client)
	if err != nil {
		zap.L().Error("bulkRequest", zap.Error(err))
		return err
	}
	if res.IsError() {
		zap.L().Error("error")
		return errors.New("error during bulkrequest")
	}

	zap.L().Info("Executing bulkindex", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

	var r elastic.BulkResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode bulk response", zap.Error(err))
		return err
	}
	if len(r.Failed()) > 0 {
		zap.L().Error("Error during bulkIndex", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))
		return errors.New("bulkindex failed > 0")
	}
	return nil
}

func ApplyMergeLight(doc models.Document, command UpdateCommand) models.Document {
	zap.L().Debug("ApplyMerge", zap.String("mergeMode", command.MergeConfig.Mode.String()), zap.Any("doc", doc), zap.Any("command", command))

	switch command.MergeConfig.Mode {
	case merge.Self:
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

func (getQuery *GetQuery) convertToExecutor() models.Document {
	alias := buildAliasName(getQuery.DocumentType, index.All)
	return *models.NewDocument(getQuery.ID, alias, "document", nil)
}

func buildAliasName(documentType string, depth index.Depth) string {
	access := fmt.Sprintf("%s-%s-%s", viper.GetString("INSTANCE_NAME"), documentType, strings.ToLower(depth.String()))
	return access
}
