package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/ingester"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v4/metrics"
	"go.uber.org/zap"
)

// IngesterHandler is a basic struct allowing to setup a single bulkIngester intance for all handlers
type IngesterHandler struct {
	bulkIngester *ingester.BulkIngester
}

// NewIngesterHandler returns a pointer to an IngesterHandler instance
func NewIngesterHandler() *IngesterHandler {
	ingesterHandler := IngesterHandler{}
	ingesterHandler.bulkIngester = ingester.NewBulkIngester(elasticsearch.C())
	return &ingesterHandler
}

// ReceiveData godoc
// @Title ReceiveData
// @Description Entrypoint for bulk data ingestion
// @tags Ingest
// @Resource /ingester
// @Router /ingester/data [post]
// @Accept json
// @Param data body ingester.BulkIngestRequest true "Bulk Ingest Request"
// @Success 200 "Status OK"
// @Failure 400 "Status Bad Request"
// @Failure 503 "Status Service Unavailable"
func (handler *IngesterHandler) ReceiveData(w http.ResponseWriter, r *http.Request) {
	zap.L().Debug("handlers.ReceiveData()")
	metrics.C().Inc("api.handlers.ingest.total.count", 1, 1.0)

	health, err := elasticsearch.C().Client.ClusterHealth().Do(context.Background())
	if err != nil && health != nil && health.Status != "red" {
		zap.L().Error("elastic.ClientHealthCheck():", zap.Error(errors.New("Elasticsearch not available")))
		metrics.C().Inc("api.handlers.ingest.error.count", 1, 1.0)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	var bir ingester.BulkIngestRequest
	err = json.NewDecoder(r.Body).Decode(&bir)
	if err != nil {
		zap.L().Warn("ReceiveData decode", zap.Error(err))
		metrics.C().Inc("api.handlers.ingest.error.count", 1, 1.0)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	startTime := time.Now()
	handler.bulkIngester.Ingest(bir)
	metrics.C().TimingDuration("api.handlers.ingest.duration", time.Since(startTime), 1.0)
	metrics.C().Inc("api.handlers.ingest.success.count", 1, 1.0)
	w.WriteHeader(http.StatusOK)
}
