package ingester

import (
	"bytes"
	"context"
	"errors"
	"github.com/alitto/pond"
	"reflect"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
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
	Uuid                                  uuid.UUID
	TypedIngester                         *TypedIngester
	ID                                    int
	Data                                  chan UpdateCommand
	metricWorkerQueueGauge                metrics.Gauge
	metricWorkerMessage                   metrics.Counter
	metricWorkerFlushDuration             metrics.Histogram
	metricWorkerBulkInsertDuration        metrics.Histogram
	metricWorkerBulkIndexDuration         metrics.Histogram
	metricWorkerApplyMergesDuration       metrics.Histogram
	metricWorkerDirectMultiGetDuration    metrics.Histogram
	metricWorkerApplyMergesSingleDuration metrics.Histogram
	metricWorkerApplyMergesInnerDuration  metrics.Histogram
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
		Uuid:                                  uuid.New(),
		TypedIngester:                         typedIngester,
		ID:                                    id,
		Data:                                  data,
		metricWorkerQueueGauge:                _metricWorkerQueueGauge.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerMessage:                   _metricWorkerMessage.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerFlushDuration:             _metricWorkerFlushDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerBulkInsertDuration:        _metricWorkerBulkInsertDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerBulkIndexDuration:         _metricWorkerBulkIndexDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerApplyMergesDuration:       _metricWorkerApplyMergesDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerDirectMultiGetDuration:    _metricWorkerDirectMultiGetDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerApplyMergesSingleDuration: _metricWorkerApplyMergesSingleDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
		metricWorkerApplyMergesInnerDuration:  _metricWorkerApplyMergesInnerDuration.With("typedingester", typedIngester.DocumentType, "workerid", strconv.Itoa(id)),
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
			zap.L().Info("Try on after timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout))
			if len(buffer) > 0 {
				zap.L().Info("Flushing on timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout))
				s := time.Now()
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
				zap.L().Info("Finished flushing on timeout reached", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Int("Timeout", forceFlushTimeout), zap.Any("duration", time.Since(s)))
			}
			worker.metricWorkerQueueGauge.Set(float64(len(worker.Data)))
			forceFlush = worker.resetForceFlush(forceFlushTimeout)

		// Build indexing bulk
		case uc := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
			buffer = append(buffer, uc)
			if len(buffer) >= bufferLength {
				zap.L().Info("Try flushing on full buffer", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)))
				s := time.Now()
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
				forceFlush = worker.resetForceFlush(forceFlushTimeout)
				zap.L().Info("Finished try flushing on full buffer", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("workerLen", len(worker.Data)), zap.Any("duration", time.Since(s)))
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

func (worker *IndexingWorkerV8) directBulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "directMultiGetDocs"))

	start := time.Now()
	refDocs, err := worker.directMultiGetDocs(updateCommandGroups)
	worker.metricWorkerDirectMultiGetDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	if err != nil {
		zap.L().Error("directMultiGetDocs", zap.Error(err))
	}

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))

	start = time.Now()
	push, err := worker.applyMergesV2(updateCommandGroups, refDocs)
	worker.metricWorkerApplyMergesDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	if err != nil {
		zap.L().Error("applyMergesV2", zap.Error(err))
	}

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))

	start = time.Now()
	err = worker.bulkIndex(push)
	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)

	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

func (worker *IndexingWorkerV8) directMultiGetDocs(updateCommandGroups [][]UpdateCommand) ([]models.Document, error) {
	docs := make([]*models.Document, 0)
	for _, updateCommandGroup := range updateCommandGroups {
		docs = append(docs, &models.Document{Index: updateCommandGroup[0].Index, ID: updateCommandGroup[0].DocumentID})
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))

	source := make(map[string]interface{})
	sourceItems := make([]types.MgetOperation, len(docs))
	for i, doc := range docs {
		sourceItems[i] = types.MgetOperation{Index_: some.String(doc.Index), Id_: doc.ID}
	}
	source["docs"] = sourceItems

	req := mget.NewRequest()
	req.Docs = sourceItems

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	response, err := elasticsearchv8.C().Mget().Request(req).Do(ctx)
	if err != nil {
		zap.L().Warn("json encode source", zap.Error(err))
	}
	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
	}

	refDocs := make([]models.Document, 0)
	for _, d := range response.Docs {
		switch typedDoc := d.(type) {
		// case types.MultiGetError:
		//     not working :(

		// case types.GetResult:
		//     not working :(

		case map[string]interface{}:
			jsonString, err := jsoniter.Marshal(typedDoc)
			if err != nil {
				zap.L().Error("update multiget unmarshal", zap.Error(err))
				refDocs = append(refDocs, models.Document{})
				continue
			}

			var typedDocOk types.GetResult
			err = jsoniter.Unmarshal(jsonString, &typedDocOk)
			if err != nil {
				zap.L().Error("update multiget unmarshal", zap.Error(err))
				refDocs = append(refDocs, models.Document{})
				continue
			}
			if len(typedDocOk.Source_) == 0 {
				// no source => MultiGetError
				refDocs = append(refDocs, models.Document{})
				continue
			}

			var source map[string]interface{}
			err = jsoniter.Unmarshal(typedDocOk.Source_, &source)
			if err != nil {
				zap.L().Error("update multiget unmarshal", zap.Error(err))
				refDocs = append(refDocs, models.Document{})
				continue
			}

			if typedDocOk.Found {
				refDocs = append(refDocs, models.Document{ID: typedDocOk.Id_, Index: typedDocOk.Index_, IndexType: "_doc", Source: source})
			} else {
				refDocs = append(refDocs, models.Document{})
			}

			// if len(refDocs) > i && refDocs[i].ID == "" {
			// 	if typedDocOk.Found {
			// 		refDocs[i] = models.Document{ID: typedDocOk.Id_, Index: typedDocOk.Index_, IndexType: "_doc", Source: source}
			// 	}
			// } else {
			// 	if typedDocOk.Found {
			// 		refDocs = append(refDocs, models.Document{ID: typedDocOk.Id_, Index: typedDocOk.Index_, IndexType: "_doc", Source: source})
			// 	} else {
			// 		refDocs = append(refDocs, models.Document{})
			// 	}
			// }
		default:
			zap.L().Error("Unkwown response type", zap.Any("typedDoc", typedDoc), zap.Any("type", reflect.TypeOf(typedDoc)))
		}
	}

	return refDocs, nil
}

// bulkChainedUpdate process multiple groups of UpdateCommand
// It execute sequentialy every single UpdateCommand on a specific "source" document, for each group of commands
func (worker *IndexingWorkerV8) bulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))
	docs := make([]GetQuery, 0)
	for _, commands := range updateCommandGroups {
		docs = append(docs, GetQuery{DocumentType: commands[0].DocumentType, ID: commands[0].DocumentID})
	}
	if len(docs) == 0 {
		zap.L().Warn("empty docs update", zap.String("typedIngesterUUID", worker.TypedIngester.Uuid.String()), zap.String("workerUUID", worker.Uuid.String()), zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))
		return
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "getindices"))
	indices, err := worker.getIndices(docs[0].DocumentType)
	if err != nil {
		zap.L().Error("getIndices", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "multiGetFindRefDocsFull"))

	refDocs, err := worker.multiGetFindRefDocsFull(indices, docs)

	if err != nil {
		zap.L().Error("multiGetFindRefDocsFull", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))
	push, err := worker.applyMerges(updateCommandGroups, refDocs)
	if err != nil {
		zap.L().Error("applyMerges", zap.Error(err))
	}

	zap.L().Debug("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))
	err = worker.bulkIndex(push)
	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}

	zap.L().Info("BulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

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
	if err := jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode alias response", zap.Error(err), zap.String("alias", alias), zap.Any("request", esapi.IndicesGetAliasRequest{Name: []string{alias}}))
		return make([]string, 0), errors.New("alias not found")
	}

	indices := make([]string, 0)
	for k := range r {
		indices = append(indices, k)
	}
	return indices, err
}

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
						refDocs = append(refDocs, models.Document{ID: typedDocOk.Id_, Index: typedDocOk.Index_, IndexType: "_doc", Source: source})
						break
					}
				default:
					zap.L().Error("Unkwown response type", zap.Any("typedDoc", typedDoc), zap.Any("type", reflect.TypeOf(typedDoc)))
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
func (worker *IndexingWorkerV8) multiGetFindRefDocs(index string, queries []GetQuery) ([]types.ResponseItem, error) {
	if len(queries) == 0 {
		return nil, errors.New("docs[] is empty")
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index))

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

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return make([]types.ResponseItem, 0), err
	}
	return response.Docs, nil
}

func (worker *IndexingWorkerV8) applyMerges(documents [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {
	var push = make([]models.Document, 0)
	var i int
	for _, commands := range documents {
		var doc models.Document
		if len(refDocs) > i {
			doc = refDocs[i]
			if doc.Index == "" {
				doc.ID = commands[0].DocumentID
				doc.Index = buildAliasName(commands[0].DocumentType, index.Last)
			}
		}

		// Index setup should probably not be here (be before in the indexing chain)
		for _, command := range commands {
			// if command.NewDoc.Index == "" {
			// 	command.NewDoc.Index = buildAliasName(command.DocumentType, index.Last)
			// }
			doc = ApplyMergeLight(doc, command)
		}

		doc.IndexType = "document"
		push = append(push, doc)
		i++ // synchronise map iteration with reponse.Docs
	}
	return push, nil
}

func (worker *IndexingWorkerV8) applyMergesV2(updateCommandGroups [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {
	zap.L().Info("ApplyMergesV2", zap.Int("workerId", worker.ID), zap.Int("updateCommandGroups size", len(updateCommandGroups)), zap.Int("refDocs size", len(refDocs)))

	push := make([]models.Document, 0)
	for i, updateCommandGroup := range updateCommandGroups {
		start2 := time.Now()

		var pushDoc models.Document
		if len(refDocs) > i {
			pushDoc = models.Document{ID: refDocs[i].ID, Index: refDocs[i].Index, IndexType: refDocs[i].IndexType, Source: refDocs[i].Source}
		}
		for _, command := range updateCommandGroup {
			if pushDoc.ID == "" {
				pushDoc = models.Document{ID: command.NewDoc.ID, Index: command.NewDoc.Index, IndexType: command.NewDoc.IndexType, Source: command.NewDoc.Source}
			} else {
				start := time.Now()
				pushDoc = ApplyMergeLight(pushDoc, command)
				worker.metricWorkerApplyMergesSingleDuration.Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
			}
		}
		push = append(push, pushDoc)
		worker.metricWorkerApplyMergesInnerDuration.Observe(float64(time.Since(start2).Nanoseconds()) / 1e9)
	}

	return push, nil
}

func (worker *IndexingWorkerV8) applyMergesV3(updateCommandGroups [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {
	workerCount := viper.GetInt("APPLY_MERGE_WORKER_COUNT")
	pool := pond.New(workerCount, 0, pond.MinWorkers(workerCount))
	push := make([]models.Document, len(updateCommandGroups))

	for idx, updateCommandGroup := range updateCommandGroups {
		// We must create a local copy of the loop variables, otherwise the goroutine will use the last values of the variables
		i := idx                                     // creates a copy of i
		updateCommandGroupPtr := &updateCommandGroup // save the pointer to the slice (to avoid useless copy)
		pool.Submit(func() {
			var pushDoc models.Document
			if len(refDocs) > i {
				pushDoc = models.Document{ID: refDocs[i].ID, Index: refDocs[i].Index, IndexType: refDocs[i].IndexType, Source: refDocs[i].Source}
			}

			for _, command := range *updateCommandGroupPtr {
				if pushDoc.ID == "" {
					pushDoc = models.Document{ID: command.NewDoc.ID, Index: command.NewDoc.Index, IndexType: command.NewDoc.IndexType, Source: command.NewDoc.Source}
				} else {
					pushDoc = ApplyMergeLight(pushDoc, command)
				}
			}

			push[i] = pushDoc
		})
	}

	// Stop the pool and wait for all submitted tasks to complete
	pool.StopAndWait()

	return push, nil
}

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

func (worker *IndexingWorkerV8) bulkIndex(docs []models.Document) error {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	start := time.Now()
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
	if err := jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
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

// func (worker *IndexingWorkerV8) bulkIndexNew(docs []models.Document) error {

// 	client := &elasticsearch.Client{
// 		BaseClient: elasticsearchv8.C().BaseClient,
// 	}
// 	client.API = esapi.New(client)

// 	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
// 		Client: client,
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	for _, doc := range docs {
// 		sourceStr, err := jsoniter.Marshal(doc.Source)
// 		if err != nil {
// 			return err
// 		}
// 		err = bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem{
// 			Index:      doc.Index,
// 			DocumentID: doc.ID,
// 			Body:       bytes.NewReader(sourceStr),
// 			// OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
// 			// 	fmt.Printf("[%d] %s test/%s", res.Status, res.Result, item.DocumentID)
// 			// },
// 			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
// 				if err != nil {
// 					zap.L().Warn("Fail to index document to elasticsearch", zap.Error(err))
// 				} else {
// 					zap.L().Warn("Fail to index document to elasticsearch",
// 						zap.String("errorType", res.Error.Type),
// 						zap.String("errorReason", res.Error.Reason))
// 				}
// 			},
// 		})
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	err = bulkIndexer.Close(context.Background())

// 	zap.L().Info("bulk", zap.Any("stats", bulkIndexer.Stats()))
// }
