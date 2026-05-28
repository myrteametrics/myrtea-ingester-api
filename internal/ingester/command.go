package ingester

import (
	"github.com/myrteametrics/myrtea-sdk/v5/connector"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
)

// UpdateCommand wrap all infos required to update a document in elasticsearch
type UpdateCommand struct {
	DocumentID   string           `json:"documentId"`
	DocumentType string           `json:"documentType"`
	NewDoc       models.Document  `json:"doc"`
	MergeConfig  connector.Config `json:"merge"`
	Index        string           `json:"index"`
	// AppendOnly disables the mget lookup and merge step for this command.
	// When true, the document is inserted directly into Elasticsearch.
	AppendOnly bool `json:"appendOnly"`
}

// NewUpdateCommand returns a new UpdateCommand
func NewUpdateCommand(index string, documentID string, documentType string, newDoc models.Document,
	mergeConfig connector.Config, appendOnly bool) UpdateCommand {
	uc := UpdateCommand{
		Index:        index,
		DocumentID:   documentID,
		DocumentType: documentType,
		NewDoc:       newDoc,
		MergeConfig:  mergeConfig,
		AppendOnly:   appendOnly,
	}
	return uc
}
