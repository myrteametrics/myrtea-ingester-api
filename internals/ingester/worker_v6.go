package ingester

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	goelasticsearch "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/go-kit/kit/metrics"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v4/index"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/olivere/elastic"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// IndexingWorkerV6 is the unit of processing which can be started in parallel for elasticsearch ingestion
type IndexingWorkerV6 struct {
	Uuid                      uuid.UUID
	TypedIngester             *TypedIngester
	ID                        int
	Data                      chan UpdateCommand
	Client                    *goelasticsearch.Client
	metricWorkerQueueGauge    metrics.Gauge
	metricWorkerMessage       metrics.Counter
	metricWorkerFlushDuration metrics.Histogram
}

// NewIndexingWorkerV6 returns a new IndexingWorkerV6
func NewIndexingWorkerV6(typedIngester *TypedIngester, id int) *IndexingWorkerV6 {

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

	worker := &IndexingWorkerV6{
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

func (worker *IndexingWorkerV6) GetData() chan UpdateCommand {
	return worker.Data
}

func (worker *IndexingWorkerV6) GetMetricWorkerQueueGauge() metrics.Gauge {
	return worker.metricWorkerQueueGauge
}

// Run start a worker
func (worker *IndexingWorkerV6) Run() {
	zap.L().Info("Starting IndexingWorkerV6",
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

func (worker *IndexingWorkerV6) resetForceFlush(sec int) <-chan time.Time {
	return time.After(time.Duration(sec) * time.Second)
}

func (worker *IndexingWorkerV6) flushEsBuffer(buffer []UpdateCommand) {
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

func (worker *IndexingWorkerV6) directBulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "directMultiGetDocs"))
	refDocs, err := worker.directMultiGetDocs(updateCommandGroups)
	if err != nil {
		zap.L().Error("directMultiGetDocs", zap.Error(err))
	}

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))
	push, err := worker.applyMergesV2(updateCommandGroups, refDocs)
	if err != nil {
		zap.L().Error("applyMergesV2", zap.Error(err))
	}

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))
	err = worker.bulkIndex(push)
	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

func (worker *IndexingWorkerV6) directMultiGetDocs(updateCommandGroups [][]UpdateCommand) ([]models.Document, error) {
	docs := make([]*models.Document, 0)
	for _, updateCommandGroup := range updateCommandGroups {
		docs = append(docs, &models.Document{Index: updateCommandGroup[0].Index, ID: updateCommandGroup[0].DocumentID})
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID))

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
	err := jsoniter.NewEncoder(body).Encode(source)
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
	if err := jsoniter.NewDecoder(res.Body).Decode(&response); err != nil {
		zap.L().Error("parsing the response body", zap.Error(err))
		// return make([]*elastic.GetResult, 0), err
	}
	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

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
func (worker *IndexingWorkerV6) bulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {

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

func (worker *IndexingWorkerV6) getIndices(documentType string) ([]string, error) {
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

func (worker *IndexingWorkerV6) multiGetFindRefDocsFull(indices []string, docs []GetQuery) ([]models.Document, error) {
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

func (worker *IndexingWorkerV6) multiGetFindRefDocs(index string, queries []GetQuery) ([]*elastic.GetResult, error) {
	if len(queries) == 0 {
		return nil, errors.New("docs[] is empty")
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index))

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
	err := jsoniter.NewEncoder(body).Encode(source)
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
	if err := jsoniter.NewDecoder(res.Body).Decode(&response); err != nil {
		zap.L().Error("parsing the response body", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}
	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("index", index), zap.String("status", "done"))

	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("MultiGet (self)", zap.Error(err))
		return make([]*elastic.GetResult, 0), err
	}
	return response.Docs, nil
}

func (worker *IndexingWorkerV6) applyMerges(documents [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {
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

func (worker *IndexingWorkerV6) applyMergesV2(updateCommandGroups [][]UpdateCommand, refDocs []models.Document) ([]models.Document, error) {

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

func (worker *IndexingWorkerV6) bulkIndex(docs []models.Document) error {

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

	//if res.IsError() {
	//	zap.L().Error("error")
	//	return errors.New("error during bulkrequest")
	//}

	if res.IsError() {
		defer res.Body.Close()
		bodyBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			zap.L().Error("error reading response body", zap.Error(err))
			return errors.New("error during bulkrequest and failed to read the response")
		}
		bodyStr := string(bodyBytes)

		// Split the string on ';' and get the first error message
		errorsList := strings.Split(bodyStr, ";")
		if len(errorsList) > 0 {
			zap.L().Error("Elasticsearch Bulk Request error", zap.String("first_error", errorsList[0]))
		} else {
			zap.L().Error("Elasticsearch Bulk Request error", zap.String("response", bodyStr))
		}

		return errors.New("error during bulkrequest")
	}

	zap.L().Debug("Executing bulkindex", zap.String("TypedIngester", worker.TypedIngester.DocumentType), zap.Int("WorkerID", worker.ID), zap.String("status", "done"))

	var r elastic.BulkResponse
	if err := jsoniter.NewDecoder(res.Body).Decode(&r); err != nil {
		zap.L().Error("decode bulk response", zap.Error(err))
		return err
	}
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
