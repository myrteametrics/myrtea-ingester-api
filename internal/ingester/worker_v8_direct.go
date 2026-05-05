package ingester

// This file contains the ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true path.
//
// In this mode the worker does NOT resolve an alias to a list of concrete
// indices.  Instead it sends a single mget with the explicit index embedded
// in each lookup operation.  This is faster when all documents live in the
// same known index, but it cannot handle multi-index aliases.
//
// Processing pipeline:
//   directBulkChainedUpdate
//     └─► directMultiGetDocs     mget with one explicit index per document
//     └─► applyDirectMerges      apply all commands in order on each refDoc
//     └─► bulkIndex              upsert via bulk "index" request

import (
	"context"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
	"go.uber.org/zap"
)

// directBulkChainedUpdate is the ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true
// entry point.  It fetches the current state of each document with a single
// mget (each operation carries an explicit index), merges the pending commands,
// then writes the results back with a bulk index request.
func (worker *IndexingWorkerV8) directBulkChainedUpdate(commandGroups [][]UpdateCommand) {
	log := func(step string) {
		zap.L().Debug("DirectBulkChainedUpdate",
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.String("step", step),
		)
	}

	log("starting")

	// Step 1 – fetch current document state (one mget, explicit index per doc).
	log("directMultiGetDocs")
	start := time.Now()
	existingDocs, err := worker.directMultiGetDocs(commandGroups)
	worker.metricWorkerDirectMultiGetDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		zap.L().Error("directMultiGetDocs failed", zap.Error(err))
	}

	// Step 2 – merge pending commands on top of the fetched state.
	log("applyDirectMerges")
	start = time.Now()
	docsToIndex := worker.applyDirectMerges(commandGroups, existingDocs)
	worker.metricWorkerApplyMergesDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	// Step 3 – write back to ES via a single bulk request.
	log("bulkIndex")
	start = time.Now()
	err = worker.bulkIndex(docsToIndex)
	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)
	if err != nil {
		zap.L().Error("bulkIndex failed", zap.Error(err))
	}

	log("done")
}

// applyDirectMerges pairs each command group with its pre-fetched reference
// document (positional match: existingDocs[i] corresponds to commandGroups[i]).
//
// If the reference document is empty (not found in ES) the first command's
// NewDoc is used as the starting state.  Subsequent commands in the group are
// then layered on top via ApplyMergeLight.
// (Used only in ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true.)
func (worker *IndexingWorkerV8) applyDirectMerges(
	commandGroups [][]UpdateCommand,
	existingDocs []models.Document,
) []models.Document {
	docsToIndex := make([]models.Document, 0, len(commandGroups))

	for i, commands := range commandGroups {
		var mergedDoc models.Document

		// Seed from the existing ES document when available.
		if i < len(existingDocs) && existingDocs[i].ID != "" {
			ref := existingDocs[i]
			mergedDoc = models.Document{
				ID:        ref.ID,
				Index:     ref.Index,
				IndexType: ref.IndexType,
				Source:    ref.Source,
			}
		}

		for _, cmd := range commands {
			if mergedDoc.ID == "" {
				// Document does not yet exist in ES – use the incoming doc directly.
				mergedDoc = models.Document{
					ID:        cmd.NewDoc.ID,
					Index:     cmd.NewDoc.Index,
					IndexType: cmd.NewDoc.IndexType,
					Source:    cmd.NewDoc.Source,
				}
			} else {
				mergedDoc = ApplyMergeLight(mergedDoc, cmd)
			}
		}

		docsToIndex = append(docsToIndex, mergedDoc)
	}

	return docsToIndex
}

// directMultiGetDocs issues a single mget request where each lookup operation
// specifies its own target index explicitly (taken from the first command in
// each group).  The result list is positionally aligned with commandGroups:
// existingDocs[i] is the current ES state for commandGroups[i][0].DocumentID.
//
// If a document is not found its slot in the result slice is an empty
// models.Document{} so callers can detect the absence by checking ID == "".
// (Used only in ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true.)
func (worker *IndexingWorkerV8) directMultiGetDocs(commandGroups [][]UpdateCommand) ([]models.Document, error) {
	// Build one MgetOperation per command group.
	mgetItems := make([]types.MgetOperation, 0, len(commandGroups))
	for _, group := range commandGroups {
		mgetItems = append(mgetItems, types.MgetOperation{
			Index_: new(group[0].Index),
			Id_:    group[0].DocumentID,
		})
	}

	zap.L().Debug("directMultiGetDocs: sending mget",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.Int("count", len(mgetItems)),
	)

	req := mget.NewRequest()
	req.Docs = mgetItems

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	response, err := worker.performMgetRequest(ctx, elasticsearch.C().Mget().Request(req))
	if err != nil {
		zap.L().Error("directMultiGetDocs: mget failed", zap.Error(err))
		return nil, err
	}
	if len(response.Docs) == 0 {
		zap.L().Warn("directMultiGetDocs: mget returned no documents")
		return nil, nil
	}

	// Convert each response item to a models.Document.
	// Positional alignment is guaranteed because ES returns items in the same
	// order as the request.
	existingDocs := make([]models.Document, 0, len(response.Docs))
	for _, item := range response.Docs {
		if !item.Found || len(item.Source_) == 0 {
			// Not found or no source (e.g. MultiGetError) – add empty placeholder.
			existingDocs = append(existingDocs, models.Document{})
			continue
		}
		existingDocs = append(existingDocs, models.Document{
			ID:        item.ID_,
			Index:     item.Index_,
			IndexType: "_doc",
			Source:    item.Source_,
		})
	}

	zap.L().Debug("directMultiGetDocs: done",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)
	return existingDocs, nil
}
