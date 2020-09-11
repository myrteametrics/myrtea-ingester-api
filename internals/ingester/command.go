package ingester

import (
	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/merge"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
)

// UpdateCommand wrap all infos required to update a document in elasticsearch
type UpdateCommand struct {
	DocumentID   string           `json:"documentId"`
	DocumentType string           `json:"documentType"`
	NewDoc       *models.Document `json:"doc"`
	MergeConfig  *merge.Config    `json:"merge"`
}

// NewUpdateCommand returns a new UpdateCommand
func NewUpdateCommand(documentID string, documentType string, newDoc *models.Document, mergeConfig *merge.Config) *UpdateCommand {
	uc := UpdateCommand{
		DocumentID:   documentID,
		DocumentType: documentType,
		NewDoc:       newDoc,
		MergeConfig:  mergeConfig,
	}
	return &uc
}
