package ingester

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/go-kit/kit/metrics"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v5/index"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const nanosPerSecond = 1e9

// IndexingWorkerV8 is the unit of processing which can be started in parallel for elasticsearch ingestion
type IndexingWorkerV8 struct {
	UUID                                     uuid.UUID
	TypedIngester                            *TypedIngester
	ID                                       int
	Data                                     chan UpdateCommand
	mgetBatchSize                            int
	metricWorkerQueueGauge                   metrics.Gauge
	metricWorkerMessage                      metrics.Counter
	metricWorkerFlushDuration                metrics.Histogram
	metricWorkerBulkInsertDuration           metrics.Histogram
	metricWorkerBulkIndexDuration            metrics.Histogram
	metricWorkerApplyMergesDuration          metrics.Histogram
	metricWorkerDirectMultiGetDuration       metrics.Histogram
	metricWorkerBulkIndexBuildBufferDuration metrics.Histogram
	metricWorkerGetIndicesDuration           metrics.Histogram
}

// NewIndexingWorkerV8 returns a new IndexingWorkerV8
func NewIndexingWorkerV8(typedIngester *TypedIngester, id, mgetBatchSize int) *IndexingWorkerV8 {
	var data chan UpdateCommand
	if workerQueueSize := viper.GetInt("WORKER_QUEUE_BUFFER_SIZE"); workerQueueSize > 0 {
		data = make(chan UpdateCommand, workerQueueSize)
	} else {
		data = make(chan UpdateCommand)
	}

	initHisto := func(h metrics.Histogram) metrics.Histogram {
		return h.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id))
	}

	initGauge := func(g metrics.Gauge) metrics.Gauge {
		return g.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id))
	}

	initCounter := func(c metrics.Counter) metrics.Counter {
		return c.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id))
	}

	worker := &IndexingWorkerV8{
		UUID:                                     uuid.New(),
		TypedIngester:                            typedIngester,
		ID:                                       id,
		mgetBatchSize:                            mgetBatchSize,
		Data:                                     data,
		metricWorkerQueueGauge:                   initGauge(_metricWorkerQueueGauge),
		metricWorkerMessage:                      initCounter(_metricWorkerMessage),
		metricWorkerFlushDuration:                initHisto(_metricWorkerFlushDuration),
		metricWorkerBulkInsertDuration:           initHisto(_metricWorkerBulkInsertDuration),
		metricWorkerBulkIndexDuration:            initHisto(_metricWorkerBulkIndexDuration),
		metricWorkerApplyMergesDuration:          initHisto(_metricWorkerApplyMergesDuration),
		metricWorkerDirectMultiGetDuration:       initHisto(_metricWorkerDirectMultiGetDuration),
		metricWorkerBulkIndexBuildBufferDuration: initHisto(_metricWorkerBulkIndexBuildBufferDuration),
		metricWorkerGetIndicesDuration:           initHisto(_metricWorkerGetIndicesDuration),
	}
	worker.metricWorkerQueueGauge.Set(0)
	worker.metricWorkerMessage.With("status", "flushed").Add(0)

	return worker
}

func (worker *IndexingWorkerV8) GetData() chan UpdateCommand {
	return worker.Data
}

func (worker *IndexingWorkerV8) GetMetricWorkerQueueGauge() metrics.Gauge {
	return worker.metricWorkerQueueGauge
}

// Run start a worker
func (worker *IndexingWorkerV8) Run() {
	zap.L().Info("Starting IndexingWorkerV8",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)

	bufferLength := viper.GetInt("WORKER_MAXIMUM_BUFFER_SIZE")
	buffer := make([]UpdateCommand, 0)

	forceFlushTimeout := viper.GetInt("WORKER_FORCE_FLUSH_TIMEOUT_SEC")
	forceFlush := worker.resetForceFlush(forceFlushTimeout)

	uid := worker.TypedIngester.UUID.String()

	for {
		select {
		// Send indexing bulk (when buffer is full or on timeout)
		case <-forceFlush:
			zap.L().Info("Try on after timeout reached", zap.String("typedIngesterUUID", uid),
				zap.String("workerUUID", worker.UUID.String()),
				zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)),
				zap.Int("workerLen", len(worker.Data)),
				zap.Int("Timeout", forceFlushTimeout))
			if len(buffer) > 0 {
				zap.L().Info("Flushing on timeout reached", zap.String("typedIngesterUUID", uid),
					zap.String("workerUUID", worker.UUID.String()),
					zap.String("TypedIngester", worker.TypedIngester.DocumentType),
					zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)),
					zap.Int("workerLen", len(worker.Data)),
					zap.Int("Timeout", forceFlushTimeout))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
			}
			forceFlush = worker.resetForceFlush(forceFlushTimeout)

		// Build indexing bulk
		case uc := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand", zap.String("typedIngesterUUID", uid),
				zap.String("workerUUID", worker.UUID.String()),
				zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
			buffer = append(buffer, uc)
			if len(buffer) >= bufferLength {
				zap.L().Info("Try flushing on full buffer", zap.String("typedIngesterUUID", uid),
					zap.String("workerUUID", worker.UUID.String()),
					zap.String("TypedIngester", worker.TypedIngester.DocumentType),
					zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)),
					zap.Int("workerLen", len(worker.Data)))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
				forceFlush = worker.resetForceFlush(forceFlushTimeout)
			}
			worker.metricWorkerQueueGauge.Set(float64(len(worker.Data)))
		}
	}
}

func (worker *IndexingWorkerV8) resetForceFlush(sec int) <-chan time.Time {
	return time.After(time.Duration(sec) * time.Second)
}

func (worker *IndexingWorkerV8) flushEsBuffer(buffer []UpdateCommand) {
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
	worker.metricWorkerFlushDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
}

// bulkChainedUpdate process multiple groups of UpdateCommand
// It execute sequentialy every single UpdateCommand on a specific "source" document, for each group of commands
// part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) bulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {
	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))

	docs := make([]GetQuery, 0)
	for _, commands := range updateCommandGroups {
		docs = append(docs, GetQuery{DocumentType: commands[0].DocumentType, ID: commands[0].DocumentID})
	}
	if len(docs) == 0 {
		zap.L().Warn("empty docs update", zap.String("typedIngesterUUID", worker.TypedIngester.UUID.String()),
			zap.String("workerUUID", worker.UUID.String()),
			zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))
		return
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "getindices"))
	start := time.Now()
	indices, err := worker.getIndices(docs[0].DocumentType)
	worker.metricWorkerGetIndicesDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		zap.L().Error("getIndices", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "multiGetFindRefDocsFull"))

	start = time.Now()
	refDocs := worker.multiGetFindRefDocsFullV2(indices, docs)
	worker.metricWorkerDirectMultiGetDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))

	start = time.Now()
	push := worker.applyMerges(updateCommandGroups, refDocs)
	worker.metricWorkerApplyMergesDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))

	start = time.Now()
	err = worker.bulkIndex(push)

	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

// getIndices part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) getIndices(documentType string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	alias := buildAliasName(documentType, index.Patch)

	res, err := esapi.IndicesGetAliasRequest{Name: []string{alias}}.Do(ctx, elasticsearch.C())
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
		Aliases map[string]any `json:"aliases"`
	})
	if err = jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode alias response", zap.Error(err), zap.String("alias", alias),
			zap.Any("request", esapi.IndicesGetAliasRequest{Name: []string{alias}}))
		return make([]string, 0), errors.New("alias not found")
	}

	indices := make([]string, 0)
	for k := range r {
		indices = append(indices, k)
	}
	return indices, err
}

// multiGetFindRefDocsFullV2 part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) multiGetFindRefDocsFullV2(indices []string,
	docs []GetQuery) map[string]models.Document {
	refDocs := map[string]models.Document{}
	var mgetBatches []map[string]GetQuery
	currentMgetBatch := map[string]GetQuery{}

	for i := range docs {
		if i != 0 && worker.mgetBatchSize != 0 && i%worker.mgetBatchSize == 0 {
			mgetBatches = append(mgetBatches, currentMgetBatch)
			currentMgetBatch = map[string]GetQuery{}
		}

		currentMgetBatch[docs[i].ID] = docs[i]
	}
	mgetBatches = append(mgetBatches, currentMgetBatch)

	// all batches must be checked on all indices,
	// so we loop on indices and then on batches
	for _, idx := range indices {
		for _, batch := range mgetBatches {
			if len(batch) == 0 {
				continue
			}

			responseDocs, err := worker.multiGetFindRefDocsV2(idx, batch)
			if err != nil {
				zap.L().Error("multiGetFindRefDocs", zap.Error(err))
			}

			for _, doc := range responseDocs.Docs {
				if !doc.Found {
					continue
				}

				// a document was found, add it to the refDocs map
				refDocs[doc.ID_] = models.Document{
					ID: doc.ID_, Index: doc.Index_,
					IndexType: "_doc", Source: doc.Source_,
				}

				// remove it from the batch
				delete(batch, doc.ID_)
			}
		}
	}

	return refDocs
}

//revive:disable:var-naming
type multiGetResponseItem struct {
	Found   bool           `json:"found"`
	ID_     string         `json:"_id"`
	Index_  string         `json:"_index"`
	Source_ map[string]any `json:"_source,omitempty"`
}

//revive:enable:var-naming

type multiGetResponse struct {
	Docs []multiGetResponseItem `json:"docs"`
}

// multiGetFindRefDocsV2 part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) multiGetFindRefDocsV2(index string,
	queries map[string]GetQuery) (*multiGetResponse, error) {
	if len(queries) == 0 {
		return nil, errors.New("docs[] is empty")
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("index", index))

	source := make(map[string]any)
	sourceItems := make([]types.MgetOperation, len(queries))
	i := 0
	for id := range queries {
		sourceItems[i] = types.MgetOperation{Id_: id}
		i++
	}
	source["docs"] = sourceItems

	req := mget.NewRequest()
	req.Docs = sourceItems

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	response, err := worker.perfomMgetRequest(ctx, elasticsearch.C().Mget().Index(index).Request(req))
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return &multiGetResponse{Docs: make([]multiGetResponseItem, 0)}, err
	}
	return response, nil
}

// performMgetRequest part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true/false
func (worker *IndexingWorkerV8) perfomMgetRequest(ctx context.Context, r *mget.Mget) (*multiGetResponse, error) {
	response := &multiGetResponse{}

	res, err := r.Perform(ctx)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 299 {
		err = jsoniter.NewDecoder(res.Body).Decode(response)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	errorResponse := types.NewElasticsearchError()
	err = jsoniter.NewDecoder(res.Body).Decode(errorResponse)
	if err != nil {
		return nil, err
	}

	return nil, errorResponse
}

// multiGetFindRefDocs part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
// func (worker *IndexingWorkerV8) multiGetFindRefDocs(index string, queries []GetQuery) ([]types.MgetResponseItem, error) {
// 	if len(queries) == 0 {
// 		return nil, errors.New("docs[] is empty")
// 	}
//
// 	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
// 		zap.Int("WorkerID", worker.ID), zap.String("index", index))
//
// 	source := make(map[string]any)
// 	sourceItems := make([]types.MgetOperation, len(queries))
// 	for i, query := range queries {
// 		sourceItems[i] = types.MgetOperation{ID_: query.ID}
// 	}
// 	source["docs"] = sourceItems
//
// 	req := mget.NewRequest()
// 	req.Docs = sourceItems
//
// 	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
// 	defer cancel()
// 	response, err := elasticsearch.C().Mget().Index(index).Request(req).Do(ctx)
// 	if err != nil {
// 		zap.L().Warn("json encode source", zap.Error(err))
// 	}
//
// 	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
// 		zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))
//
// 	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
// 		zap.L().Error("MultiGet (self)", zap.Error(err))
// 		return make([]types.MgetResponseItem, 0), err
// 	}
// 	return response.Docs, nil
// }

// applyMerges part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) applyMerges(documents [][]UpdateCommand,
	refDocs map[string]models.Document) []models.Document {
	var push = make([]models.Document, 0)

	for _, commands := range documents {
		var doc models.Document

		docID := commands[0].DocumentID

		if found, ok := refDocs[docID]; ok && found.Index != "" {
			doc = refDocs[docID]
		} else {
			doc = models.Document{
				ID:    docID,
				Index: buildAliasName(commands[0].DocumentType, index.Last),
			}
		}

		// Index setup should probably not be here (be before in the indexing chain)
		for _, command := range commands {
			doc = ApplyMergeLight(doc, command)
		}

		doc.IndexType = "document"
		push = append(push, doc)
	}

	return push
}

// buildBulkIndexItem all modes: ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false/true
func buildBulkIndexItem(index string, id string, source any) ([]string, error) {
	lines := make([]string, 2)

	meta := elasticsearch.BulkIndexMeta{
		Index: elasticsearch.BulkIndexMetaDetail{
			S_Index: index,
			S_Id:    id,
		},
	}

	line0, err := jsoniter.Marshal(meta)
	if err != nil {
		return nil, err
	}
	lines[0] = string(line0)

	line1, err := jsoniter.Marshal(source)
	if err != nil {
		return nil, err
	}
	lines[1] = string(line1)

	return lines, nil
}

// bulkIndex all modes: ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false/true
func (worker *IndexingWorkerV8) bulkIndex(docs []models.Document) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	buf := bytes.NewBuffer(make([]byte, 0))

	for _, doc := range docs {
		source, err := buildBulkIndexItem(doc.Index, doc.ID, doc.Source)
		if err != nil {
			zap.L().Error("", zap.Error(err))
		}
		for _, line := range source {
			buf.WriteString(line)
			buf.WriteByte('\n')
		}
	}
	worker.metricWorkerBulkIndexBuildBufferDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	start = time.Now()
	res, err := esapi.BulkRequest{Body: buf}.Do(ctx, elasticsearch.C())
	worker.metricWorkerBulkInsertDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	if err != nil {
		zap.L().Error("bulkRequest", zap.Error(err))
		return err
	}
	if res.IsError() {
		zap.L().Error("error", zap.Strings("warnings", res.Warnings()), zap.String("response", res.String()))
		return errors.New("error during bulkrequest")
	}

	zap.L().Debug("Executing bulkindex", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

	// var r map[string]any
	var r elasticsearch.BulkIndexResponse
	if err = jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode bulk response", zap.Error(err))
		return err
	}

	if len(r.Failed()) > 0 {
		zap.L().Warn("Error during bulkIndex", zap.String("typedIngesterUUID", worker.TypedIngester.UUID.String()),
			zap.String("workerUUID", worker.UUID.String()),
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID), zap.Int("Docs", len(docs)),
			zap.Int("Errors", len(r.Failed())))
		sampleItemFound := false
		for _, item := range r.Items {
			if item["index"].Error.Type == "" {
				continue
			}
			zap.L().Warn("BulkIndex ElasticSearch error", zap.Any("error", item["index"].Error))
			sampleItemFound = true
			break
		}

		if len(r.Items) > 0 && !sampleItemFound {
			zap.L().Warn("Error item sample", zap.Any("response", r.Items[0]))
		}

		errorsMap := make(map[string]int64)
		for _, item := range r.Items {
			errorsMap[item["index"].Error.Type]++
		}
		zap.L().Warn("Error typology mapping", zap.Any("errors", errorsMap))
		return errors.New("bulkindex failed > 0")
	}
	return nil
}
