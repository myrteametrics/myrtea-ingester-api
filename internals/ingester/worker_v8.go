package ingester

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/go-kit/kit/metrics"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearchv8"
	"github.com/myrteametrics/myrtea-sdk/v4/index"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// IndexingWorkerV8 is the unit of processing which can be started in parallel for elasticsearch ingestion
type IndexingWorkerV8 struct {
	Uuid                                     uuid.UUID
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
func NewIndexingWorkerV8(typedIngester *TypedIngester, id int) *IndexingWorkerV8 {

	var data chan UpdateCommand
	if workerQueueSize := viper.GetInt("WORKER_QUEUE_BUFFER_SIZE"); workerQueueSize > 0 {
		data = make(chan UpdateCommand, workerQueueSize)
	} else {
		data = make(chan UpdateCommand)
	}

	worker := &IndexingWorkerV8{
		Uuid:                                     uuid.New(),
		TypedIngester:                            typedIngester,
		ID:                                       id,
		Data:                                     data,
		metricWorkerQueueGauge:                   _metricWorkerQueueGauge.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerMessage:                      _metricWorkerMessage.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerFlushDuration:                _metricWorkerFlushDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerBulkInsertDuration:           _metricWorkerBulkInsertDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerBulkIndexDuration:            _metricWorkerBulkIndexDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerApplyMergesDuration:          _metricWorkerApplyMergesDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerDirectMultiGetDuration:       _metricWorkerDirectMultiGetDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerBulkIndexBuildBufferDuration: _metricWorkerBulkIndexBuildBufferDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerGetIndicesDuration:           _metricWorkerGetIndicesDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
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
			zap.L().Info("Try on after timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()),
				zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)),
				zap.Int("Timeout", forceFlushTimeout))
			if len(buffer) > 0 {
				zap.L().Info("Flushing on timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()),
					zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType),
					zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)),
					zap.Int("Timeout", forceFlushTimeout))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
			}
			forceFlush = worker.resetForceFlush(forceFlushTimeout)

		// Build indexing bulk
		case uc := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()),
				zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
			buffer = append(buffer, uc)
			if len(buffer) >= bufferLength {
				zap.L().Info("Try flushing on full buffer", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()),
					zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType),
					zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)))
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
	worker.metricWorkerFlushDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
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
		zap.L().Warn("empty docs update", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()),
			zap.String("workerUUID", worker.Uuid.String()),
			zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))
		return
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "getindices"))
	start := time.Now()
	indices, err := worker.getIndices(docs[0].DocumentType)
	worker.metricWorkerGetIndicesDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
	if err != nil {
		zap.L().Error("getIndices", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "multiGetFindRefDocsFull"))

	start = time.Now()
	refDocs, err := worker.multiGetFindRefDocsFullV2(indices, docs)
	worker.metricWorkerDirectMultiGetDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	if err != nil {
		zap.L().Error("multiGetFindRefDocsFull", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))

	start = time.Now()
	push, err := worker.applyMerges(updateCommandGroups, refDocs)
	worker.metricWorkerApplyMergesDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	if err != nil {
		zap.L().Error("applyMerges", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))

	start = time.Now()
	err = worker.bulkIndex(push)
	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
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

	res, err := esapi.IndicesGetAliasRequest{Name: []string{alias}}.Do(ctx, elasticsearchv8.C())
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

// multiGetFindRefDocsFull part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) multiGetFindRefDocsFullV2(indices []string, docs []GetQuery) (map[string]models.Document, error) {
	refDocs := map[string]models.Document{}
	var mgetBatches []map[string]GetQuery

	// create batches
	currentMgetBatch := map[string]GetQuery{}

	for i := 0; i < len(docs); i++ {
		if i != 0 && i%worker.mgetBatchSize == 0 {
			mgetBatches = append(mgetBatches, currentMgetBatch)
			currentMgetBatch = map[string]GetQuery{}
		}

		currentMgetBatch[docs[i].ID] = docs[i]
	}

	// all batches must be checked on all indices,
	// so we loop on indices and then on batches
	for _, index := range indices {
		for _, batch := range mgetBatches {
			if len(batch) == 0 {
				continue
			}

			responseDocs, err := worker.multiGetFindRefDocsV2(index, batch)
			if err != nil {
				zap.L().Error("multiGetFindRefDocs", zap.Error(err))
			}

			for _, doc := range responseDocs.Docs {
				if !doc.Found {
					continue
				}

				// a document was found, add it to the refDocs map
				refDocs[doc.Id_] = models.Document{ID: doc.Id_, Index: doc.Index_, IndexType: "_doc", Source: doc.Source_}

				// remove it from the batch
				delete(batch, doc.Id_)
			}

			// Should we?
			mgetBatches = worker.reorderBatches(mgetBatches)

		}
	}

	return refDocs, nil
}

func (worker *IndexingWorkerV8) reorderBatches(mgetBatch []map[string]GetQuery) []map[string]GetQuery {
	// here we reorder the batches, so that the first batch is the one with the most found documents (to avoid useless requests)
	// the first must always have the most but limited by worker.mgetBatchSize
	if len(mgetBatch) <= 1 {
		return mgetBatch
	}
	for i := 1; i < len(mgetBatch); i++ {
		// check if batch is full
		if len(mgetBatch[i]) >= worker.mgetBatchSize {
			continue
		}

		// check if we have a next batch, continue else
		if i+1 >= len(mgetBatch) {
			continue
		}

		// move as much as possible from the next batch to the current one (limited by worker.mgetBatchSize)
		for k, v := range mgetBatch[i+1] {
			if len(mgetBatch[i]) >= worker.mgetBatchSize {
				break
			}
			mgetBatch[i][k] = v
			delete(mgetBatch[i+1], k)
		}
	}
	return mgetBatch
}

type multiGetResponseItem struct {
	//Fields       map[string]jsoniter.RawMessage `json:"fields,omitempty"`
	Found  bool   `json:"found"`
	Id_    string `json:"_id"`
	Index_ string `json:"_index"`
	//PrimaryTerm_ *int64                         `json:"_primary_term,omitempty"`
	//Routing_     *string                        `json:"_routing,omitempty"`
	//SeqNo_       *int64                         `json:"_seq_no,omitempty"`
	Source_ map[string]interface{} `json:"_source,omitempty"`
	//Version_     *int64                         `json:"_version,omitempty"`
}

type multiGetResponse struct {
	Docs []multiGetResponseItem `json:"docs"`
}

// multiGetFindRefDocs part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) multiGetFindRefDocsV2(index string, queries map[string]GetQuery) (*multiGetResponse, error) {
	if len(queries) == 0 {
		return nil, errors.New("docs[] is empty")
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index))

	source := make(map[string]interface{})
	sourceItems := make([]types.MgetOperation, len(queries))
	i := 0
	for id, _ := range queries {
		sourceItems[i] = types.MgetOperation{Id_: id}
		i++
	}
	source["docs"] = sourceItems

	req := mget.NewRequest()
	req.Docs = sourceItems

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	response, err := worker.perfomMgetRequest(elasticsearchv8.C().Mget().Index(index).Request(req), ctx)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return &multiGetResponse{Docs: make([]multiGetResponseItem, 0)}, err
	}
	return response, nil
}

// performMgetRequest part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) perfomMgetRequest(r *mget.Mget, ctx context.Context) (*multiGetResponse, error) {
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

// multiGetFindRefDocsFull part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) multiGetFindRefDocsFull(indices []string, docs []GetQuery) ([]models.Document, error) {
	refDocs := make([]models.Document, 0)

	var findDocs bool
	for _, doc := range docs {
		sliceDoc := []GetQuery{doc}
		findDocs = false
		for _, index := range indices {
			responseDocs, err := worker.multiGetFindRefDocs(index, sliceDoc)
			if err != nil {
				zap.L().Error("multiGetFindRefDocs", zap.Error(err))
			}
			for i, d := range responseDocs {
				_ = i
				switch typedDoc := d.(type) {
				case map[string]interface{}:
					jsonString, err := jsoniter.Marshal(typedDoc)
					if err != nil {
						zap.L().Error("update multiget unmarshal", zap.Error(err))
						continue
					}

					var typedDocOk types.GetResult
					err = jsoniter.Unmarshal(jsonString, &typedDocOk)
					if err != nil {
						zap.L().Error("update multiget unmarshal", zap.Error(err))
						continue
					}
					if len(typedDocOk.Source_) == 0 {
						continue
					}

					var source map[string]interface{}
					err = jsoniter.Unmarshal(typedDocOk.Source_, &source)
					if err != nil {
						zap.L().Error("update multiget unmarshal", zap.Error(err))
						continue
					}

					if typedDocOk.Found {
						findDocs = true
						refDocs = append(refDocs, models.Document{
							ID: typedDocOk.Id_, Index: typedDocOk.Index_, IndexType: "_doc", Source: source,
						})
						break
					}
				default:
					zap.L().Error("Unknown response type", zap.Any("typedDoc", typedDoc),
						zap.Any("type", reflect.TypeOf(typedDoc)))
				}

			}

			if findDocs {
				break
			}
		}
		if !findDocs {
			refDocs = append(refDocs, models.Document{})
		}
	}

	return refDocs, nil
}

// multiGetFindRefDocs part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) multiGetFindRefDocs(index string, queries []GetQuery) ([]types.ResponseItem, error) {
	if len(queries) == 0 {
		return nil, errors.New("docs[] is empty")
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("index", index))

	source := make(map[string]interface{})
	sourceItems := make([]types.MgetOperation, len(queries))
	for i, query := range queries {
		sourceItems[i] = types.MgetOperation{Id_: query.ID}
	}
	source["docs"] = sourceItems

	req := mget.NewRequest()
	req.Docs = sourceItems

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	response, err := elasticsearchv8.C().Mget().Index(index).Request(req).Do(ctx)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return make([]types.ResponseItem, 0), err
	}
	return response.Docs, nil
}

// applyMerges part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false
func (worker *IndexingWorkerV8) applyMerges(documents [][]UpdateCommand, refDocs map[string]models.Document) ([]models.Document, error) {
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

	return push, nil
}

// buildBulkIndexItem all modes: ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false/true
func buildBulkIndexItem(index string, id string, source interface{}) ([]string, error) {
	lines := make([]string, 2)

	meta := elasticsearchv8.BulkIndexMeta{
		Index: elasticsearchv8.BulkIndexMetaDetail{
			S_Index: index,
			S_Type:  "_doc",
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
	worker.metricWorkerBulkIndexBuildBufferDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	start = time.Now()
	res, err := esapi.BulkRequest{Body: buf}.Do(ctx, elasticsearchv8.C())
	worker.metricWorkerBulkInsertDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	if err != nil {
		zap.L().Error("bulkRequest", zap.Error(err))
		return err
	}
	if res.IsError() {
		zap.L().Error("error")
		return errors.New("error during bulkrequest")
	}

	zap.L().Debug("Executing bulkindex", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

	// var r map[string]interface{}
	var r elasticsearchv8.BulkIndexResponse
	if err = jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode bulk response", zap.Error(err))
		return err
	}

	// zap.L().Info("response", zap.Any("r", r))
	if len(r.Failed()) > 0 {
		zap.L().Warn("Error during bulkIndex", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Docs", len(docs)), zap.Int("Errors", len(r.Failed())))
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
			if _, found := errorsMap[item["index"].Error.Type]; found {
				errorsMap[item["index"].Error.Type] += 1
			} else {
				errorsMap[item["index"].Error.Type] = 1
			}
		}
		zap.L().Warn("Error typology mapping", zap.Any("errors", errorsMap))
		return errors.New("bulkindex failed > 0")
	}
	return nil
}
