package ingester

import (
	"github.com/myrteametrics/myrtea-sdk/v5/connector"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
)

// IngestRequest wrap a single ingestion request (one document with one mergeconfig)
type IngestRequest struct {
	UUID         string           `json:"uuid"`
	BulkUUID     string           `json:"bulkUuid"`
	DocumentType string           `json:"documentType"`
	MergeConfig  connector.Config `json:"merge"`
	Doc          models.Document  `json:"docs"`
}

// BulkIngestRequest wrap a collection of ingestion request (multiple documents with multiple mergeconfigs)
type BulkIngestRequest struct {
	UUID         string             `json:"uuid"`
	DocumentType string             `json:"documentType"`
	MergeConfig  []connector.Config `json:"merge"`
	Docs         []models.Document  `json:"docs"`
}
