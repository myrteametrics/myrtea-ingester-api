package ingester

import (
	"strconv"
	"time"

	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/merge"
	ttlcache "github.com/myrteametrics/myrtea-sdk/v4/cache"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearch"
	"go.uber.org/zap"
)

// BulkIngester is a component which split BulkIngestRequest and affect the resulting IngestRequests to dedicated TypedIngester
// As a chokepoint, it doesn't do much processing and only acts as a request router
type BulkIngester struct {
	EsExecutor     *elasticsearch.EsExecutor
	TypedIngesters map[string]*TypedIngester
	Cache          *ttlcache.Cache
}

// NewBulkIngester returns a pointer to a new BulkIngester instance
func NewBulkIngester(esExecutor *elasticsearch.EsExecutor) *BulkIngester {
	ingester := BulkIngester{}
	ingester.EsExecutor = esExecutor
	ingester.TypedIngesters = make(map[string]*TypedIngester)
	ingester.Cache = ttlcache.NewCache(3600 * time.Second)
	return &ingester
}

// getTypedIngester returns a pointer to the required TypedIngester
// If the required TypedIngester doesn't exists, it will be created and started
func (ingester *BulkIngester) getTypedIngester(targetDocumentType string) *TypedIngester {
	typedIngester, ok := ingester.TypedIngesters[targetDocumentType]
	if !ok {
		typedIngester = NewTypedIngester(ingester, targetDocumentType)
		ingester.TypedIngesters[targetDocumentType] = typedIngester
		go typedIngester.Run()
		time.Sleep(10 * time.Millisecond) // goroutine warm-up
		zap.L().Info("New TypedIngester created and started", zap.String("targetDocumentType", targetDocumentType))
	}
	return typedIngester
}

// Ingest process a single BulkIngestRequest
// The BulkIngestRequest is splitted in multiple IngestRequest, then sent to a specific TypedIngester
// The target TypedIngester is selected, based on which document type must be updated
func (ingester *BulkIngester) Ingest(bir BulkIngestRequest) {
	zap.L().Debug("Processing BulkIngestRequest", zap.String("BulkUUID", bir.UUID))

	for _, mergeConfig := range bir.MergeConfig {
		var targetDocumentType string
		switch mergeConfig.Mode {
		case merge.Self, merge.EnrichFrom:
			targetDocumentType = bir.DocumentType
		case merge.EnrichTo:
			// The document to update is not the current document but another one
			targetDocumentType = mergeConfig.Type
		default:
			zap.L().Error("Unknown merge mode, skipping...", zap.String("BulkUUID", bir.UUID), zap.String("mode", mergeConfig.Mode.String()))
			continue
		}

		typedIngester := ingester.getTypedIngester(targetDocumentType)

		for i, doc := range bir.Docs {
			ir := IngestRequest{BulkUUID: bir.UUID, UUID: strconv.Itoa(i), DocumentType: bir.DocumentType, MergeConfig: mergeConfig, Doc: doc}
			zap.L().Debug("Send IngestRequest", zap.String("BulkUUID", bir.UUID), zap.Int("RequestUUID", i), zap.Any("IngestRequest", ir))
			typedIngester.Data <- &ir
		}
	}
}
