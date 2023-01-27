package ingester

import (
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/merge"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
)

// UpdateCommand wrap all infos required to update a document in elasticsearch
type UpdateCommand struct {
	DocumentID   string           `json:"documentId"`
	DocumentType string           `json:"documentType"`
	NewDoc       *models.Document `json:"doc"`
	MergeConfig  *merge.Config    `json:"merge"`
	Index        string           `json:"index"`
}

// NewUpdateCommand returns a new UpdateCommand
func NewUpdateCommand(index string, documentID string, documentType string, newDoc *models.Document, mergeConfig *merge.Config) *UpdateCommand {
	uc := UpdateCommand{
		Index:        index,
		DocumentID:   documentID,
		DocumentType: documentType,
		NewDoc:       newDoc,
		MergeConfig:  mergeConfig,
	}
	return &uc
}
