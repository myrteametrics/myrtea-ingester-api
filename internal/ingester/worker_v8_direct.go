package ingester

import (
	"context"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
	"go.uber.org/zap"
)

// directBulkChainedUpdate part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true
func (worker *IndexingWorkerV8) directBulkChainedUpdate(updateCommandGroups [][]UpdateCommand) {
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "starting"))
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "directMultiGetDocs"))

	start := time.Now()
	refDocs, err := worker.directMultiGetDocs(updateCommandGroups)
	worker.metricWorkerDirectMultiGetDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	if err != nil {
		zap.L().Error("directMultiGetDocs", zap.Error(err))
	}

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "applyMerges"))

	start = time.Now()
	push := worker.applyDirectMerges(updateCommandGroups, refDocs)
	worker.metricWorkerApplyMergesDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "bulkIndex"))

	start = time.Now()
	err = worker.bulkIndex(push)
	worker.metricWorkerBulkIndexDuration.Observe(float64(time.Since(start).Nanoseconds()) / nanosPerSecond)

	if err != nil {
		zap.L().Error("bulkIndex", zap.Error(err))
	}
	zap.L().Debug("DirectBulkChainUpdate", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("step", "done"))
}

// applyDirectMerges part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true
func (worker *IndexingWorkerV8) applyDirectMerges(updateCommandGroups [][]UpdateCommand,
	refDocs []models.Document) []models.Document {
	push := make([]models.Document, 0)
	for i, updateCommandGroup := range updateCommandGroups {
		var pushDoc models.Document
		if len(refDocs) > i {
			pushDoc = models.Document{
				ID:        refDocs[i].ID,
				Index:     refDocs[i].Index,
				IndexType: refDocs[i].IndexType,
				Source:    refDocs[i].Source,
			}
		}
		for _, command := range updateCommandGroup {
			if pushDoc.ID == "" {
				pushDoc = models.Document{
					ID:        command.NewDoc.ID,
					Index:     command.NewDoc.Index,
					IndexType: command.NewDoc.IndexType,
					Source:    command.NewDoc.Source,
				}
			} else {
				pushDoc = ApplyMergeLight(pushDoc, command)
			}
		}
		push = append(push, pushDoc)
	}

	return push
}

// directMultiGetDocs part of ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true
func (worker *IndexingWorkerV8) directMultiGetDocs(updateCommandGroups [][]UpdateCommand) ([]models.Document, error) {
	docs := make([]*models.Document, 0)
	for _, updateCommandGroup := range updateCommandGroups {
		docs = append(docs, &models.Document{Index: updateCommandGroup[0].Index, ID: updateCommandGroup[0].DocumentID})
	}

	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID))

	source := make(map[string]any)
	sourceItems := make([]types.MgetOperation, len(docs))
	for i, doc := range docs {
		sourceItems[i] = types.MgetOperation{Index_: some.String(doc.Index), Id_: doc.ID}
	}
	source["docs"] = sourceItems

	req := mget.NewRequest()
	req.Docs = sourceItems

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	zap.L().Debug("Executing multiget", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID), zap.String("status", "done"))
	response, err := worker.perfomMgetRequest(ctx, elasticsearch.C().Mget().Request(req))
	if err != nil || response.Docs == nil || len(response.Docs) == 0 {
		zap.L().Error("perfomMgetRequest (self)", zap.Error(err))
	}

	refDocs := make([]models.Document, 0)
	for _, d := range response.Docs {
		if len(d.Source_) == 0 {
			// no source => MultiGetError
			refDocs = append(refDocs, models.Document{})
			continue
		}

		if d.Found {
			refDocs = append(refDocs, models.Document{ID: d.ID_, Index: d.Index_, IndexType: "_doc", Source: d.Source_})
		} else {
			refDocs = append(refDocs, models.Document{})
		}
	}

	return refDocs, nil
}
