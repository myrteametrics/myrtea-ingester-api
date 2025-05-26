package handlers

import (
	"errors"
	jsoniter "github.com/json-iterator/go"
	"net/http"

	"github.com/myrteametrics/myrtea-ingester-api/v5/internal/ingester"

	"go.uber.org/zap"
)

// IngesterHandler is a basic struct allowing to setup a single bulkIngester intance for all handlers
type IngesterHandler struct {
	bulkIngester *ingester.BulkIngester
}

// NewIngesterHandler returns a pointer to an IngesterHandler instance
func NewIngesterHandler() *IngesterHandler {
	return &IngesterHandler{
		bulkIngester: ingester.NewBulkIngester(),
	}
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
	// ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	// defer cancel()
	// health, err := elasticsearch.C().Client.ClusterHealth().Do(ctx)
	// if err != nil && health != nil && health.Status != "red" {
	// 	zap.L().Error("elastic.ClientHealthCheck():", zap.Error(errors.New("elasticsearch not available")))
	// 	w.WriteHeader(http.StatusServiceUnavailable)
	// 	return
	// }

	var bir ingester.BulkIngestRequest

	err := jsoniter.NewDecoder(r.Body).Decode(&bir)
	if err != nil {
		zap.L().Warn("Cannot decode body", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = handler.bulkIngester.Ingest(bir)
	if err != nil {
		if errors.Is(err, ingester.ErrChannelOverload) {
			w.WriteHeader(http.StatusTooManyRequests)
		} else if err.Error() == "elasticsearch healthcheck red" { // Replace with custom error
			w.WriteHeader(http.StatusInternalServerError)
		} else if errors.Is(err, ingester.ErrDocumentTypeEmpty) {
			w.WriteHeader(http.StatusBadRequest)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}
