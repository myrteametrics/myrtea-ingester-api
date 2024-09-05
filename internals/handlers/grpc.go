package handlers

import (
	"context"
	"fmt"

	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/ingester"
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/protobuf/pb"
	"github.com/myrteametrics/myrtea-sdk/v5/connector"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
)

type IngesterServer struct {
	pb.UnimplementedIngesterServer
	bulkIngester *ingester.BulkIngester
}

// NewIngesterServer returns a pointer to an IngesterHandler instance
func NewIngesterServer() IngesterServer {
	return IngesterServer{
		bulkIngester: ingester.NewBulkIngester(),
	}
}

func (s IngesterServer) Ingest(ctx context.Context, bir *pb.BulkIngestRequest) (*pb.BulkIngestResponse, error) {
	fmt.Println("Ingest()")
	mergeConfigs := make([]connector.Config, 0)
	for _, mergeConfig := range bir.GetMergeConfigs() {
		groups := make([]connector.Group, 0)
		for _, group := range mergeConfig.GetGroups() {
			fieldMaths := make([]connector.FieldMath, 0)
			for _, fieldMath := range group.GetFieldMath() {
				fieldMaths = append(fieldMaths, connector.FieldMath{
					Expression:  fieldMath.GetExpression(),
					OutputField: fieldMath.GetOutputField(),
				})
			}
			groups = append(groups, connector.Group{
				Condition:             group.GetCondition(),
				FieldReplace:          group.GetFieldReplace(),
				FieldReplaceIfMissing: group.GetFieldReplaceIfMissing(),
				FieldMerge:            group.GetFieldMerge(),
				FieldKeepLatest:       group.GetFieldKeepLatest(),
				FieldKeepEarliest:     group.GetFieldKeepEarliest(),
				FieldForceUpdate:      group.GetFieldForceUpdate(),
				FieldMath:             fieldMaths,
			})
		}

		mergeConfigs = append(mergeConfigs, connector.Config{
			Mode:             connector.Self, // mergeConfig.Mode
			ExistingAsMaster: mergeConfig.GetExistingAsMaster(),
			Type:             mergeConfig.GetType(),
			LinkKey:          mergeConfig.GetLinkKey(),
			Groups:           groups,
		})
	}

	docs := make([]models.Document, 0)
	for _, doc := range bir.GetDocuments() {
		docs = append(docs, models.Document{
			ID:        doc.GetID(),
			Index:     doc.GetIndex(),
			IndexType: doc.GetIndexType(),
			Source:    doc.GetSource().AsMap(),
		})
	}

	internalBir := ingester.BulkIngestRequest{
		UUID:         bir.GetUUID(),
		DocumentType: bir.GetDocumentType(),
		MergeConfig:  mergeConfigs,
		Docs:         docs,
	}

	err := s.bulkIngester.Ingest(internalBir)
	if err != nil {
		return nil, err
	}

	return &pb.BulkIngestResponse{}, nil
}
