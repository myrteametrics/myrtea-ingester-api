package ingester

// ╔═════════════════════════════════════════════════════════════════════════════╗
// ║              IndexingWorkerV8 – Data Processing Pipeline                    ║
// ╠═════════════════════════════════════════════════════════════════════════════╣
// ║                                                                             ║
// ║  This file implements the two "regular-update" ingestion modes as well as   ║
// ║  the "append-only" fast-path.  See the ASCII diagram below for the full     ║
// ║  processing flow inside a single worker goroutine.                          ║
// ║                                                                             ║
// ║  ┌─────────────────────────────────────────────────────────────────────┐    ║
// ║  │                    Worker.Run() – event loop                        │    ║
// ║  │                                                                     │    ║
// ║  │   chan UpdateCommand  ──► accumulate in buffer                      │    ║
// ║  │                              │                                      │    ║
// ║  │          buffer full? OR force-flush timeout?                       │    ║
// ║  │                              │                                      │    ║
// ║  │                      flushEsBuffer()                                │    ║
// ║  │                              │                                      │    ║
// ║  │       ┌──────────────────────┴──────────────────────┐               │    ║
// ║  │  AppendOnly=false                          AppendOnly=true          │    ║
// ║  │  (processed FIRST)                         (processed AFTER)        │    ║
// ║  │       │                                             │               │    ║
// ║  │       ▼                                             ▼               │    ║
// ║  │  group by DocumentID                     appendOnlyBulkIndex()      │    ║
// ║  │       │                                  (bulk "create" – fails     │    ║
// ║  │       │                                   on duplicate ID)          │    ║
// ║  │       │                                             │               │    ║
// ║  │  ELASTICSEARCH_DIRECT_MULTI_GET_MODE?               │               │    ║
// ║  │       │                                             │               │    ║
// ║  │  ┌────┴────┐                                        │               │    ║
// ║  │  │  true   │  false                                 │               │    ║
// ║  │  │         │                                        │               │    ║
// ║  │  ▼         ▼                                        │               │    ║
// ║  │  directMultiGetDocs   getIndices()                  │               │    ║
// ║  │  (mget with explicit  (resolve patch alias          │               │    ║
// ║  │   index per doc)       → concrete index names)      │               │    ║
// ║  │  │         │                                        │               │    ║
// ║  │  │         ▼                                        │               │    ║
// ║  │  │   multiGetFindRefDocsFullV2()                    │               │    ║
// ║  │  │   (batched mget across all indices)              │               │    ║
// ║  │  │         │                                        │               │    ║
// ║  │  ▼         ▼                                        │               │    ║
// ║  │  applyDirectMerges   applyMerges                    │               │    ║
// ║  │  (ordered list)      (map lookup by DocumentID)     │               │    ║
// ║  │  │         │                                        │               │    ║
// ║  │  └────┬────┘                                        │               │    ║
// ║  │       ▼                                             │               │    ║
// ║  │   bulkIndex()          ◄────────────────────────────┘               │    ║
// ║  │   (bulk "index" / upsert semantics)                                 │    ║
// ║  │                              │                                      │    ║
// ║  │                       Elasticsearch                                 │    ║
// ║  └─────────────────────────────────────────────────────────────────────┘    ║
// ║                                                                             ║
// ║  Configuration knobs                                                        ║
// ║  ─────────────────────────────────────────────────────────────────────────  ║
// ║  WORKER_QUEUE_BUFFER_SIZE            size of the per-worker channel buffer  ║
// ║  WORKER_MAXIMUM_BUFFER_SIZE          trigger flush when buffer reaches this ║
// ║  WORKER_FORCE_FLUSH_TIMEOUT_SEC      flush at least every N seconds         ║
// ║  ELASTICSEARCH_DIRECT_MULTI_GET_MODE skip alias resolution (direct mget)    ║
// ║  ELASTICSEARCH_MGET_BATCH_SIZE       docs per mget batch (0 = unbounded)    ║
// ║  DEBUG_DRY_RUN_ELASTICSEARCH         skip all ES writes (metrics still run) ║
// ╚═════════════════════════════════════════════════════════════════════════════╝

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

// nanosPerSecond converts time.Duration nanoseconds to seconds for Prometheus histograms.
const nanosPerSecond = 1e9

// ─── Worker struct ─────────────────────────────────────────────────────────────

// IndexingWorkerV8 is the unit of processing that can be started in parallel
// for Elasticsearch ingestion.  Each worker owns an independent buffered channel
// and drains it in a single goroutine, which prevents concurrent writes to the
// same document within one worker.
type IndexingWorkerV8 struct {
	UUID          uuid.UUID
	TypedIngester *TypedIngester
	ID            int
	// Data is the inbound channel; the TypedIngester routes commands here.
	Data chan UpdateCommand
	// mgetBatchSize caps how many documents are looked up in a single mget call.
	// 0 means "no limit" (all docs in one request).
	mgetBatchSize int

	// ── Prometheus metrics ────────────────────────────────────────────────────
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

// ─── Constructor ───────────────────────────────────────────────────────────────

// NewIndexingWorkerV8 constructs and returns an initialised IndexingWorkerV8.
// All Prometheus metrics are pre-labelled with the document type and worker ID.
func NewIndexingWorkerV8(typedIngester *TypedIngester, id, mgetBatchSize int) *IndexingWorkerV8 {
	var dataChan chan UpdateCommand
	if workerQueueSize := viper.GetInt("WORKER_QUEUE_BUFFER_SIZE"); workerQueueSize > 0 {
		dataChan = make(chan UpdateCommand, workerQueueSize)
	} else {
		dataChan = make(chan UpdateCommand)
	}

	labelWith := func(labels ...string) []string {
		return append([]string{"typedingester", typedIngester.DocumentType, labelWorkerID, strconv.Itoa(id)}, labels...)
	}

	worker := &IndexingWorkerV8{
		UUID:          uuid.New(),
		TypedIngester: typedIngester,
		ID:            id,
		mgetBatchSize: mgetBatchSize,
		Data:          dataChan,

		metricWorkerQueueGauge:                   _metricWorkerQueueGauge.With(labelWith()...),
		metricWorkerMessage:                      _metricWorkerMessage.With(labelWith()...),
		metricWorkerFlushDuration:                _metricWorkerFlushDuration.With(labelWith()...),
		metricWorkerBulkInsertDuration:           _metricWorkerBulkInsertDuration.With(labelWith()...),
		metricWorkerBulkIndexDuration:            _metricWorkerBulkIndexDuration.With(labelWith()...),
		metricWorkerApplyMergesDuration:          _metricWorkerApplyMergesDuration.With(labelWith()...),
		metricWorkerDirectMultiGetDuration:       _metricWorkerDirectMultiGetDuration.With(labelWith()...),
		metricWorkerBulkIndexBuildBufferDuration: _metricWorkerBulkIndexBuildBufferDuration.With(labelWith()...),
		metricWorkerGetIndicesDuration:           _metricWorkerGetIndicesDuration.With(labelWith()...),
	}

	// Initialise counters/gauges to 0 so Prometheus series are visible immediately.
	worker.metricWorkerQueueGauge.Set(0)
	worker.metricWorkerMessage.With("status", "flushed").Add(0)

	return worker
}

// ─── IndexingWorker interface ──────────────────────────────────────────────────

func (worker *IndexingWorkerV8) GetData() chan UpdateCommand {
	return worker.Data
}

func (worker *IndexingWorkerV8) GetMetricWorkerQueueGauge() metrics.Gauge {
	return worker.metricWorkerQueueGauge
}

// ─── Event loop ────────────────────────────────────────────────────────────────

// Run starts the worker's event loop.  It accumulates UpdateCommands in an
// in-memory buffer and triggers flushEsBuffer either when the buffer reaches
// WORKER_MAXIMUM_BUFFER_SIZE or when WORKER_FORCE_FLUSH_TIMEOUT_SEC elapses,
// whichever comes first.
func (worker *IndexingWorkerV8) Run() {
	zap.L().Info("Starting IndexingWorkerV8",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)

	maxBufferSize := viper.GetInt("WORKER_MAXIMUM_BUFFER_SIZE")
	buffer := make([]UpdateCommand, 0, maxBufferSize)

	forceFlushSec := viper.GetInt("WORKER_FORCE_FLUSH_TIMEOUT_SEC")
	flushTimer := time.NewTimer(time.Duration(forceFlushSec) * time.Second)
	defer flushTimer.Stop()

	ingesterUID := worker.TypedIngester.UUID.String()

	for {
		select {
		case <-flushTimer.C:
			worker.logFlushEvent("timeout", ingesterUID, len(buffer), forceFlushSec)
			if len(buffer) > 0 {
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
			}
			flushTimer.Reset(time.Duration(forceFlushSec) * time.Second)

		case cmd := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand",
				zap.String("typedIngesterUUID", ingesterUID),
				zap.String("workerUUID", worker.UUID.String()),
				zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID),
				zap.Any("UpdateCommand", cmd),
			)
			buffer = append(buffer, cmd)

			if len(buffer) >= maxBufferSize {
				worker.logFlushEvent("full buffer", ingesterUID, len(buffer), forceFlushSec)
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
				// Stop + drain the timer before resetting to avoid a spurious
				// fire if the timer fires between Stop and Reset.
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(time.Duration(forceFlushSec) * time.Second)
			}
			worker.metricWorkerQueueGauge.Set(float64(len(worker.Data)))
		}
	}
}

// logFlushEvent emits a structured log line explaining why a flush is about to happen.
func (worker *IndexingWorkerV8) logFlushEvent(reason, ingesterUID string, buffered, timeoutSec int) {
	zap.L().Info("Flushing worker buffer",
		zap.String("reason", reason),
		zap.String("typedIngesterUUID", ingesterUID),
		zap.String("workerUUID", worker.UUID.String()),
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.Int("bufferedMessages", buffered),
		zap.Int("channelLen", len(worker.Data)),
		zap.Int("timeoutSec", timeoutSec),
	)
}

// ─── Flush pipeline ────────────────────────────────────────────────────────────

// flushEsBuffer dispatches the accumulated buffer to Elasticsearch.
//
// The buffer is first partitioned into two groups:
//   - regular commands   (AppendOnly=false) → mget + merge + bulk index
//   - append-only commands (AppendOnly=true) → direct bulk create (no mget)
//
// Regular commands are processed BEFORE append-only ones so that the mget
// reads the Elasticsearch state that existed at the time those commands were
// queued, preserving causal ordering.
func (worker *IndexingWorkerV8) flushEsBuffer(buffer []UpdateCommand) {
	if len(buffer) == 0 {
		return
	}

	start := time.Now()

	// Partition the buffer.
	// Pre-allocate at half capacity each – in most workloads the buffer is
	// either all-regular or all-append-only, so this is a safe lower bound.
	regularCmds := make([]UpdateCommand, 0, len(buffer))
	appendOnlyCmds := make([]UpdateCommand, 0)
	for _, cmd := range buffer {
		if cmd.AppendOnly {
			appendOnlyCmds = append(appendOnlyCmds, cmd)
		} else {
			regularCmds = append(regularCmds, cmd)
		}
	}

	if viper.GetBool("DEBUG_DRY_RUN_ELASTICSEARCH") {
		// Dry-run mode: count messages but skip all ES I/O.
		worker.metricWorkerMessage.With("status", "flushed").Add(float64(len(buffer)))
		worker.metricWorkerFlushDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
		return
	}

	// 1. Regular updates (mget → merge → bulk index).
	if len(regularCmds) > 0 {
		// Group commands that target the same document so that all updates for
		// a given ID are applied sequentially on top of the same fetched source.
		byDocID := groupCommandsByDocumentID(regularCmds)

		if viper.GetBool("ELASTICSEARCH_DIRECT_MULTI_GET_MODE") {
			worker.directBulkChainedUpdate(byDocID)
		} else {
			worker.bulkChainedUpdate(byDocID)
		}
	}

	// 2. Append-only inserts (skip mget; bulk create).
	if len(appendOnlyCmds) > 0 {
		worker.appendOnlyBulkIndex(appendOnlyCmds)
	}

	worker.metricWorkerMessage.With("status", "flushed").Add(float64(len(buffer)))
	worker.metricWorkerFlushDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
}

// groupCommandsByDocumentID groups a flat list of UpdateCommands into slices
// where every slice shares the same DocumentID.  The order within each slice
// reflects the order in which the commands arrived.
func groupCommandsByDocumentID(commands []UpdateCommand) [][]UpdateCommand {
	orderMap := make(map[string][]UpdateCommand, len(commands))
	for _, cmd := range commands {
		orderMap[cmd.DocumentID] = append(orderMap[cmd.DocumentID], cmd)
	}
	groups := make([][]UpdateCommand, 0, len(orderMap))
	for _, group := range orderMap {
		groups = append(groups, group)
	}
	return groups
}

// ─── Append-only path ──────────────────────────────────────────────────────────

// appendOnlyBulkIndex handles AppendOnly=true commands.
// It skips the mget lookup and merge steps entirely and sends a single bulk
// "create" request.  The "create" action returns a 409 conflict instead of
// silently overwriting an existing document, which is the desired behaviour for
// append-only data streams such as event logs.
func (worker *IndexingWorkerV8) appendOnlyBulkIndex(commands []UpdateCommand) {
	zap.L().Debug("AppendOnlyBulkIndex",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.Int("count", len(commands)),
	)

	docsToInsert := make([]models.Document, 0, len(commands))
	for _, cmd := range commands {
		// Use the document's own index when specified; fall back to the
		// type-level "last-write" alias only when the caller left it empty.
		targetIndex := cmd.NewDoc.Index
		if targetIndex == "" {
			targetIndex = buildAliasName(cmd.DocumentType, index.Last)
		}
		docsToInsert = append(docsToInsert, models.Document{
			ID:        cmd.NewDoc.ID,
			Index:     targetIndex,
			IndexType: indexTypeDocument,
			Source:    cmd.NewDoc.Source,
		})
	}

	start := time.Now()
	err := worker.bulkCreate(docsToInsert)
	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		zap.L().Error("appendOnlyBulkIndex failed", zap.Error(err))
	}
}

// ─── Regular-update path (ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false) ───────────
//
//  bulkChainedUpdate
//    └─► getIndices                  resolve the patch alias → concrete index names
//    └─► multiGetFindRefDocsFullV2   fetch current document state (batched mget)
//    └─► applyMerges                 sequentially apply all commands per document
//    └─► bulkIndex                   write merged documents back to ES

// bulkChainedUpdate processes groups of UpdateCommands in
// ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false.
// It uses the patch alias to discover which concrete indices exist, then
// performs batched multi-get requests across those indices to fetch the current
// state of each document before merging.
func (worker *IndexingWorkerV8) bulkChainedUpdate(commandGroups [][]UpdateCommand) {
	log := func(step string) {
		zap.L().Debug("BulkChainedUpdate",
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.String("step", step),
		)
	}

	log("starting")

	// Build the list of (documentType, ID) pairs we need to fetch.
	lookupKeys := make([]GetQuery, 0, len(commandGroups))
	for _, group := range commandGroups {
		lookupKeys = append(lookupKeys, GetQuery{
			DocumentType: group[0].DocumentType,
			ID:           group[0].DocumentID,
		})
	}
	if len(lookupKeys) == 0 {
		zap.L().Warn("bulkChainedUpdate: empty lookup keys – nothing to do",
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
		)
		return
	}

	// Step 1 – resolve alias to concrete index names.
	log("getIndices")
	start := time.Now()
	concreteIndices, err := worker.getIndices(lookupKeys[0].DocumentType)
	worker.metricWorkerGetIndicesDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		// ⚠ Abort: without the alias resolution we cannot run mget, so we
		// cannot safely merge.  Log and drop this batch rather than blindly
		// overwriting existing documents with partial data.
		zap.L().Error("bulkChainedUpdate: getIndices failed – aborting flush",
			zap.Error(err),
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.Int("affectedGroups", len(commandGroups)),
		)
		return
	}

	// Step 2 – fetch the current document state from ES.
	log("multiGetFindRefDocsFull")
	start = time.Now()
	existingDocs := worker.multiGetFindRefDocsFullV2(concreteIndices, lookupKeys)
	worker.metricWorkerDirectMultiGetDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	// Step 3 – apply all pending commands on top of each document's current state.
	log("applyMerges")
	start = time.Now()
	docsToIndex := worker.applyMerges(commandGroups, existingDocs)
	worker.metricWorkerApplyMergesDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	// Step 4 – write back to ES via a single bulk request.
	log("bulkIndex")
	start = time.Now()
	err = worker.bulkIndex(docsToIndex)
	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		zap.L().Error("bulkIndex failed", zap.Error(err))
	}

	log("done")
}

// getIndices resolves the patch alias for a document type to the list of
// concrete index names that currently back it.
// (Used only in ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false)
func (worker *IndexingWorkerV8) getIndices(documentType string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	patchAlias := buildAliasName(documentType, index.Patch)

	res, err := esapi.IndicesGetAliasRequest{Name: []string{patchAlias}}.Do(ctx, elasticsearch.C())
	if err != nil {
		zap.L().Error("getIndices: alias request failed", zap.Error(err), zap.String("alias", patchAlias))
		return nil, errors.New("alias not found")
	}
	defer res.Body.Close()

	if res.IsError() {
		zap.L().Error("getIndices: alias not found", zap.String("alias", patchAlias))
		return nil, errors.New("alias not found")
	}

	// Response shape: { "<index-name>": { "aliases": { ... } }, ... }
	var aliasResponse map[string]struct {
		Aliases map[string]any `json:"aliases"`
	}
	if err = jsoniter.NewDecoder(res.Body).Decode(&aliasResponse); err != nil {
		zap.L().Error("getIndices: failed to decode alias response",
			zap.Error(err), zap.String("alias", patchAlias))
		return nil, errors.New("alias not found")
	}

	concreteIndices := make([]string, 0, len(aliasResponse))
	for indexName := range aliasResponse {
		concreteIndices = append(concreteIndices, indexName)
	}
	return concreteIndices, nil
}

// multiGetFindRefDocsFullV2 fetches the current source of every document in
// docs across all provided indices, using batched mget requests.
//
// The returned map is keyed by document ID.  Documents not found in any index
// are simply absent from the map (the caller then treats them as new documents).
// (Used only in ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false.)
func (worker *IndexingWorkerV8) multiGetFindRefDocsFullV2(
	indices []string,
	docs []GetQuery,
) map[string]models.Document {
	existingDocs := make(map[string]models.Document)

	// Build batches so we never send more than mgetBatchSize keys per request.
	batches := worker.buildMgetBatches(docs)

	// Every batch must be checked against every index because a document may
	// live in any one of the indices that back the alias.
	for _, idx := range indices {
		for _, batch := range batches {
			if len(batch) == 0 {
				continue
			}

			response, err := worker.multiGetFindRefDocsV2(idx, batch)
			if err != nil {
				zap.L().Error("multiGetFindRefDocsFullV2: mget failed", zap.Error(err))
				continue
			}

			for _, item := range response.Docs {
				if !item.Found {
					continue
				}
				// Document found – record it and remove from the batch so
				// subsequent indices don't overwrite it.
				existingDocs[item.ID_] = models.Document{
					ID:        item.ID_,
					Index:     item.Index_,
					IndexType: "_doc",
					Source:    item.Source_,
				}
				delete(batch, item.ID_)
			}
		}
	}

	return existingDocs
}

// buildMgetBatches splits docs into map-based batches of at most mgetBatchSize.
// If mgetBatchSize is 0 all docs are placed in a single batch.
func (worker *IndexingWorkerV8) buildMgetBatches(docs []GetQuery) []map[string]GetQuery {
	var batches []map[string]GetQuery
	current := make(map[string]GetQuery)

	for i, doc := range docs {
		if i != 0 && worker.mgetBatchSize != 0 && i%worker.mgetBatchSize == 0 {
			batches = append(batches, current)
			current = make(map[string]GetQuery)
		}
		current[doc.ID] = doc
	}
	batches = append(batches, current)
	return batches
}

// ─── mget response types ───────────────────────────────────────────────────────

// mgetResponseItem represents a single document entry in an Elasticsearch
// mget response.  The trailing underscore on field names mirrors the ES SDK
// convention for reserved JSON keys (_id, _index, _source).
//
//revive:disable:var-naming
type mgetResponseItem struct {
	Found   bool           `json:"found"`
	ID_     string         `json:"_id"`
	Index_  string         `json:"_index"`
	Source_ map[string]any `json:"_source,omitempty"`
}

//revive:enable:var-naming

type mgetResponse struct {
	Docs []mgetResponseItem `json:"docs"`
}

// multiGetFindRefDocsV2 executes one mget request against a single index and
// returns the raw response.
// (Used only in ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false.)
func (worker *IndexingWorkerV8) multiGetFindRefDocsV2(
	targetIndex string,
	queries map[string]GetQuery,
) (*mgetResponse, error) {
	if len(queries) == 0 {
		return nil, errors.New("multiGetFindRefDocsV2: queries map is empty")
	}

	zap.L().Debug("multiGetFindRefDocsV2: sending mget",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.String("index", targetIndex),
		zap.Int("count", len(queries)),
	)

	mgetItems := make([]types.MgetOperation, 0, len(queries))
	for id := range queries {
		mgetItems = append(mgetItems, types.MgetOperation{Id_: id})
	}

	req := mget.NewRequest()
	req.Docs = mgetItems

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	response, err := worker.performMgetRequest(ctx, elasticsearch.C().Mget().Index(targetIndex).Request(req))
	if err != nil {
		zap.L().Error("multiGetFindRefDocsV2: mget failed", zap.Error(err))
		return &mgetResponse{Docs: make([]mgetResponseItem, 0)}, err
	}
	if len(response.Docs) == 0 {
		return &mgetResponse{Docs: make([]mgetResponseItem, 0)}, nil
	}

	zap.L().Debug("multiGetFindRefDocsV2: done",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.String("index", targetIndex),
	)
	return response, nil
}

// performMgetRequest executes an mget request and decodes the response body
// into an mgetResponse.  It is shared between both multi-get modes.
func (worker *IndexingWorkerV8) performMgetRequest(ctx context.Context, r *mget.Mget) (*mgetResponse, error) {
	res, err := r.Perform(ctx)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 299 {
		var response mgetResponse
		if err = jsoniter.NewDecoder(res.Body).Decode(&response); err != nil {
			return nil, err
		}
		return &response, nil
	}

	// Non-2xx – decode as an Elasticsearch error object.
	esErr := types.NewElasticsearchError()
	if err = jsoniter.NewDecoder(res.Body).Decode(esErr); err != nil {
		return nil, err
	}
	return nil, esErr
}

// ─── Merge helpers ─────────────────────────────────────────────────────────────

// applyMerges iterates over each group of UpdateCommands and applies them
// sequentially on top of the document's current state (fetched by
// multiGetFindRefDocsFullV2 and passed in via existingDocs).
//
// Documents not present in existingDocs are treated as new and are written to
// the type-level "last-write" alias.
// (Used only in ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false.)
func (worker *IndexingWorkerV8) applyMerges(
	commandGroups [][]UpdateCommand,
	existingDocs map[string]models.Document,
) []models.Document {
	docsToIndex := make([]models.Document, 0, len(commandGroups))

	for _, commands := range commandGroups {
		docID := commands[0].DocumentID

		var mergedDoc models.Document
		if existing, found := existingDocs[docID]; found && existing.Index != "" {
			// Start from the document that currently lives in ES.
			mergedDoc = existing
		} else {
			// New document – target the "last-write" alias so ES picks the
			// active write index automatically.
			mergedDoc = models.Document{
				ID:    docID,
				Index: buildAliasName(commands[0].DocumentType, index.Last),
			}
		}

		// Apply every command in arrival order.
		for _, cmd := range commands {
			mergedDoc = ApplyMergeLight(mergedDoc, cmd)
		}
		mergedDoc.IndexType = "document"

		docsToIndex = append(docsToIndex, mergedDoc)
	}

	return docsToIndex
}

// ─── Bulk write helpers ────────────────────────────────────────────────────────

// buildBulkCreateItem serialises one document as the two NDJSON lines required
// for a bulk "create" action.
//
// Unlike "index", the "create" action returns a 409 conflict when a document
// with the same _id already exists, preventing silent overwrites for
// append-only workloads.
//
// When id is empty the "_id" field is omitted from the meta line entirely,
// which instructs Elasticsearch to auto-generate a unique ID.
// Passing an explicit empty string ("_id":"") would trigger an
// illegal_argument_exception.
func buildBulkCreateItem(targetIndex, id string, source any) ([]string, error) {
	// Use a local struct with omitempty on _id so that an empty id causes the
	// field to be omitted rather than serialised as "" (which ES rejects).
	type bulkCreateDetail struct {
		Index string `json:"_index"`
		ID    string `json:"_id,omitempty"`
	}
	type bulkCreateMeta struct {
		Create bulkCreateDetail `json:"create"`
	}

	metaLine, err := jsoniter.Marshal(bulkCreateMeta{
		Create: bulkCreateDetail{Index: targetIndex, ID: id},
	})
	if err != nil {
		return nil, err
	}

	sourceLine, err := jsoniter.Marshal(source)
	if err != nil {
		return nil, err
	}

	return []string{string(metaLine), string(sourceLine)}, nil
}

// bulkCreate sends a single ES bulk request using "create" actions for every
// document in docs.  It is used exclusively by appendOnlyBulkIndex.
//
// TODO(reliability): there is no retry logic.  A transient 429 / 503 response
// from ES causes documents to be silently dropped.  Consider an exponential
// back-off retry with a dead-letter queue for persistent failures.
func (worker *IndexingWorkerV8) bulkCreate(docs []models.Document) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build the NDJSON payload.
	start := time.Now()
	buf := bytes.NewBuffer(make([]byte, 0, len(docs)*256))
	for _, doc := range docs {
		lines, err := buildBulkCreateItem(doc.Index, doc.ID, doc.Source)
		if err != nil {
			zap.L().Error("bulkCreate: failed to serialise document", zap.Error(err), zap.String("id", doc.ID))
			continue
		}
		for _, line := range lines {
			buf.WriteString(line)
			buf.WriteByte('\n')
		}
	}
	worker.metricWorkerBulkIndexBuildBufferDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	// Send the bulk request.
	start = time.Now()
	res, err := esapi.BulkRequest{Body: buf}.Do(ctx, elasticsearch.C())
	worker.metricWorkerBulkInsertDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	if err != nil {
		zap.L().Error("bulkCreate: request failed", zap.Error(err))
		return err
	}
	if res.IsError() {
		zap.L().Error("bulkCreate: ES returned an error",
			zap.Strings("warnings", res.Warnings()),
			zap.String("response", res.String()),
		)
		return errors.New("error during bulk create request")
	}

	zap.L().Debug("bulkCreate: done",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)

	var bulkResp elasticsearch.BulkIndexResponse
	if err = jsoniter.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		zap.L().Error("bulkCreate: failed to decode response", zap.Error(err))
		return err
	}

	if failed := bulkResp.Failed(); len(failed) > 0 {
		zap.L().Warn("bulkCreate: some documents failed",
			zap.String("typedIngesterUUID", worker.TypedIngester.UUID.String()),
			zap.String("workerUUID", worker.UUID.String()),
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.Int("totalDocs", len(docs)),
			zap.Int("failedDocs", len(failed)),
		)

		// Aggregate error types so the log is concise.
		errorCounts := make(map[string]int64)
		for _, item := range bulkResp.Items {
			if t := item["create"].Error.Type; t != "" {
				errorCounts[t]++
			}
		}
		zap.L().Warn("bulkCreate: error type breakdown", zap.Any("errors", errorCounts))
		return errors.New("bulkCreate: one or more documents failed")
	}
	return nil
}

// buildBulkIndexItem serialises one document as the two NDJSON lines required
// for a bulk "index" action (upsert semantics – overwrites if the ID exists).
// Used by both ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false and =true.
//
// When id is empty the "_id" field is omitted so ES auto-generates the ID.
func buildBulkIndexItem(targetIndex, id string, source any) ([]string, error) {
	// Same omitempty trick as buildBulkCreateItem: an explicit "" causes an
	// illegal_argument_exception on the ES side.
	type bulkIndexDetail struct {
		Index string `json:"_index"`
		ID    string `json:"_id,omitempty"`
	}
	type bulkIndexMeta struct {
		Index bulkIndexDetail `json:"index"`
	}

	metaLine, err := jsoniter.Marshal(bulkIndexMeta{
		Index: bulkIndexDetail{Index: targetIndex, ID: id},
	})
	if err != nil {
		return nil, err
	}

	sourceLine, err := jsoniter.Marshal(source)
	if err != nil {
		return nil, err
	}

	return []string{string(metaLine), string(sourceLine)}, nil
}

// bulkIndex sends a single ES bulk request using "index" actions (upsert).
// Used by both ELASTICSEARCH_DIRECT_MULTI_GET_MODE=false and =true.
//
// TODO(reliability): no retry.  A 429/503 silently drops the entire batch.
// TODO(es): consider adding "require_alias": true to the bulk meta so that a
// misconfigured write on a concrete index (instead of the alias) fails loudly.
func (worker *IndexingWorkerV8) bulkIndex(docs []models.Document) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build the NDJSON payload.
	start := time.Now()
	buf := bytes.NewBuffer(make([]byte, 0, len(docs)*256))
	for _, doc := range docs {
		lines, err := buildBulkIndexItem(doc.Index, doc.ID, doc.Source)
		if err != nil {
			zap.L().Error("bulkIndex: failed to serialise document", zap.Error(err), zap.String("id", doc.ID))
			continue
		}
		for _, line := range lines {
			buf.WriteString(line)
			buf.WriteByte('\n')
		}
	}
	worker.metricWorkerBulkIndexBuildBufferDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	// Send the bulk request.
	start = time.Now()
	res, err := esapi.BulkRequest{Body: buf}.Do(ctx, elasticsearch.C())
	worker.metricWorkerBulkInsertDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	if err != nil {
		zap.L().Error("bulkIndex: request failed", zap.Error(err))
		return err
	}
	if res.IsError() {
		zap.L().Error("bulkIndex: ES returned an error",
			zap.Strings("warnings", res.Warnings()),
			zap.String("response", res.String()),
		)
		return errors.New("error during bulk index request")
	}

	zap.L().Debug("bulkIndex: done",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)

	var bulkResp elasticsearch.BulkIndexResponse
	if err = jsoniter.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		zap.L().Error("bulkIndex: failed to decode response", zap.Error(err))
		return err
	}

	if failed := bulkResp.Failed(); len(failed) > 0 {
		zap.L().Warn("bulkIndex: some documents failed",
			zap.String("typedIngesterUUID", worker.TypedIngester.UUID.String()),
			zap.String("workerUUID", worker.UUID.String()),
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.Int("totalDocs", len(docs)),
			zap.Int("failedDocs", len(failed)),
		)

		// Log one example error item for quick diagnosis.
		for _, item := range bulkResp.Items {
			if item["index"].Error.Type != "" {
				zap.L().Warn("bulkIndex: sample error item", zap.Any("error", item["index"].Error))
				break
			}
		}

		// Aggregate error types.
		errorCounts := make(map[string]int64)
		for _, item := range bulkResp.Items {
			errorCounts[item["index"].Error.Type]++
		}
		zap.L().Warn("bulkIndex: error type breakdown", zap.Any("errors", errorCounts))
		return errors.New("bulkIndex: one or more documents failed")
	}
	return nil
}
