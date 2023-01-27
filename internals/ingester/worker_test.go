package ingester

import (
	"context"
	"testing"

	config "github.com/myrteametrics/myrtea-ingester-api/v5/internals/configuration"
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/merge"
	"github.com/myrteametrics/myrtea-sdk/v4/configuration"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/spf13/viper"
)

func TestDirectBulkChainedUpdate2(t *testing.T) {
	configuration.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	configuration.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorker(typedIngester, 1)
	// indexingWorker := IndexingWorker{}

	t.Log(indexingWorker.Client.Client.DeleteIndex("myindex", "myotherindex", "myotherotherindex").Do(context.Background()))

	docs := []*models.Document{
		{IndexType: "document", Index: "myindex", ID: "1", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myindex", ID: "2", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myotherindex", ID: "4", Source: map[string]interface{}{"a": "a", "b": "b"}},
	}
	indexingWorker.bulkIndex(docs)
	t.Fail()
}

func TestDirectBulkChainedUpdate(t *testing.T) {
	configuration.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	configuration.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorker(typedIngester, 1)
	// indexingWorker := IndexingWorker{}

	buffer := []*UpdateCommand{
		{
			Index: "myindex", DocumentID: "1", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myindex", ID: "1", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "1", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myindex", ID: "1", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "1", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myindex", ID: "1", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myindex", ID: "2", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myindex", ID: "2", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myindex", ID: "2", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},

		{
			Index: "myotherindex", DocumentID: "3", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},

		{
			Index: "myotherotherindex", DocumentID: "5", DocumentType: "document",
			MergeConfig: &merge.Config{Type: "document", Mode: merge.Self, ExistingAsMaster: true, Groups: []merge.Group{}},
			NewDoc:      &models.Document{Index: "myotherotherindex", ID: "5", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
	}

	updateCommandGroups := make(map[string][]*UpdateCommand)
	for _, uc := range buffer {
		if updateCommandGroups[uc.DocumentID] != nil {
			updateCommandGroups[uc.DocumentID] = append(updateCommandGroups[uc.DocumentID], uc)
		} else {
			updateCommandGroups[uc.DocumentID] = []*UpdateCommand{uc}
		}
	}

	indexingWorker.DirectBulkChainedUpdate(updateCommandGroups)
	t.Fail()

}
