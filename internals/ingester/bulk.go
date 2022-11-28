package ingester

import (
	"errors"
	"strconv"
	"time"

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
	return &BulkIngester{
		EsExecutor:     esExecutor,
		TypedIngesters: make(map[string]*TypedIngester),
		Cache:          ttlcache.NewCache(3600 * time.Second),
	}
}

// getTypedIngester returns a pointer to the required TypedIngester
// If the required TypedIngester doesn't exists, it will be created and started
func (ingester *BulkIngester) getTypedIngester(targetDocumentType string) *TypedIngester {
	typedIngester, found := ingester.TypedIngesters[targetDocumentType]
	if !found {
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
func (ingester *BulkIngester) Ingest(bir BulkIngestRequest) error {
	zap.L().Debug("Processing BulkIngestRequest", zap.String("BulkUUID", bir.UUID))

	mergeConfig := bir.MergeConfig[0]

	typedIngester := ingester.getTypedIngester(bir.DocumentType)

	if len(typedIngester.Data)+len(bir.Docs) >= cap(typedIngester.Data) {
		zap.L().Debug("Buffered channel would be overloaded with incoming bulkIngestRequest")
		return errors.New("channel overload") // Replace with custom error
	}

	for i, doc := range bir.Docs {
		ir := IngestRequest{BulkUUID: bir.UUID, UUID: strconv.Itoa(i), DocumentType: bir.DocumentType, MergeConfig: mergeConfig, Doc: doc}
		zap.L().Debug("Send IngestRequest", zap.String("BulkUUID", bir.UUID), zap.Int("RequestUUID", i), zap.Any("IngestRequest", ir), zap.Any("len(chan)", len(typedIngester.Data)))
		typedIngester.Data <- &ir
	}

	return nil
}
