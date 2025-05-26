package ingester

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	config "github.com/myrteametrics/myrtea-ingester-api/v5/internal/configuration"
	"github.com/myrteametrics/myrtea-sdk/v5/connector"
	"github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v5/helpers"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
	"github.com/spf13/viper"
	"testing"
)

func TestDirectBulkChainedUpdate2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping worker api calls in short mode")
	}
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1, 500)
	// indexingWorker := IndexingWorker{}

	esapi.IndicesDeleteRequest{AllowNoIndices: esapi.BoolPtr(true), Index: []string{"myindex"}}.Do(context.Background(), elasticsearch.C())
	esapi.IndicesDeleteRequest{AllowNoIndices: esapi.BoolPtr(true), Index: []string{"myotherindex"}}.Do(context.Background(), elasticsearch.C())
	esapi.IndicesDeleteRequest{AllowNoIndices: esapi.BoolPtr(true), Index: []string{"myotherotherindex"}}.Do(context.Background(), elasticsearch.C())

	docs := []models.Document{
		{IndexType: "document", Index: "myindex", ID: "1", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myindex", ID: "2", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myotherindex", ID: "4", Source: map[string]interface{}{"a": "a", "b": "b"}},
	}
	indexingWorker.bulkIndex(docs)
	t.Fail()
}

func TestDirectBulkChainedUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping worker api calls in short mode")
	}
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1, 500)
	// indexingWorker := IndexingWorker{}

	mergeConfig := connector.Config{Type: "document", Mode: connector.Self, ExistingAsMaster: true, Groups: []connector.Group{{FieldReplace: []string{"update"}}}}
	buffer := []UpdateCommand{
		{
			Index: "myindex", DocumentID: "1", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myindex", ID: "1", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "1", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myindex", ID: "1", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "1", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myindex", ID: "1", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myindex", ID: "2", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myindex", ID: "2", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myindex", ID: "2", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},

		{
			Index: "myotherindex", DocumentID: "3", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},

		{
			Index: "myotherotherindex", DocumentID: "5", DocumentType: "document", MergeConfig: mergeConfig,
			NewDoc: models.Document{Index: "myotherotherindex", ID: "5", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		},
	}

	updateCommandGroupsMap := make(map[string][]UpdateCommand)
	for _, uc := range buffer {
		if updateCommandGroupsMap[uc.DocumentID] != nil {
			updateCommandGroupsMap[uc.DocumentID] = append(updateCommandGroupsMap[uc.DocumentID], uc)
		} else {
			updateCommandGroupsMap[uc.DocumentID] = []UpdateCommand{uc}
		}
	}

	updateCommandGroups := make([][]UpdateCommand, 0)
	for _, v := range updateCommandGroupsMap {
		updateCommandGroups = append(updateCommandGroups, v)
	}

	indexingWorker.directBulkChainedUpdate(updateCommandGroups)
	t.Fail()

}

// buildBulkIndexItem
func TestBuildBulkIndexItem(t *testing.T) {
	str, err := buildBulkIndexItem("testindex", "1", map[string]interface{}{
		"a": "a",
		"b": "b",
	})
	if err != nil {
		t.Errorf("Error building bulk index item: %s", err)
	} else {
		t.Log(str)
	}
}
