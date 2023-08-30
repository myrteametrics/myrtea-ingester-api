package ingester

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	config "github.com/myrteametrics/myrtea-ingester-api/v5/internals/configuration"
	"github.com/myrteametrics/myrtea-sdk/v4/connector"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearchv8"
	"github.com/myrteametrics/myrtea-sdk/v4/helpers"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/spf13/viper"
)

var cfgv8 = elasticsearch.Config{
	Addresses: []string{
		"http://localhost:9200",
	},

	RetryOnStatus: []int{502, 503, 504},
	// EnableRetryOnTimeout: true,
	MaxRetries: math.MaxInt,
	// RetryBackoff: func(attempt int) time.Duration {},

	Transport: &http.Transport{
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second,
		// ...
	},
	// ...
}
func TestDirectBulkChainedUpdate2(t *testing.T) {
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV6(typedIngester, 1)
	// indexingWorker := IndexingWorker{}

	esapi.IndicesDeleteRequest{AllowNoIndices: esapi.BoolPtr(true), Index: []string{"myindex"}}.Do(context.Background(), elasticsearchv8.C())
	esapi.IndicesDeleteRequest{AllowNoIndices: esapi.BoolPtr(true), Index: []string{"myotherindex"}}.Do(context.Background(), elasticsearchv8.C())
	esapi.IndicesDeleteRequest{AllowNoIndices: esapi.BoolPtr(true), Index: []string{"myotherotherindex"}}.Do(context.Background(), elasticsearchv8.C())

	docs := []models.Document{
		{IndexType: "document", Index: "myindex", ID: "1", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myindex", ID: "2", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myotherindex", ID: "4", Source: map[string]interface{}{"a": "a", "b": "b"}},
	}
	indexingWorker.bulkIndex(docs)
	t.Fail()
}

func TestDirectBulkChainedUpdate(t *testing.T) {
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV6(typedIngester, 1)
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

func TestNewIndexWorker(t *testing.T) {
	typedIngester := &TypedIngester{DocumentType: "test"}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1)
	if indexingWorker.TypedIngester.DocumentType != "test" {
		t.Fail()
	}
}

func TestBulkChainedUpdate(t *testing.T) {
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1)
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

		// {
		// 	Index: "myotherindex", DocumentID: "3", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },
		// {
		// 	Index: "myotherindex", DocumentID: "3", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },
		// {
		// 	Index: "myotherindex", DocumentID: "3", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherindex", ID: "3", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },
		// {
		// 	Index: "myotherindex", DocumentID: "4", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },
		// {
		// 	Index: "myotherindex", DocumentID: "4", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },
		// {
		// 	Index: "myotherindex", DocumentID: "4", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherindex", ID: "4", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },

		// {
		// 	Index: "myotherotherindex", DocumentID: "5", DocumentType: "document", MergeConfig: mergeConfig,
		// 	NewDoc: models.Document{Index: "myotherotherindex", ID: "5", IndexType: "document", Source: map[string]interface{}{"update": "data"}},
		// },
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

	indexingWorker.bulkChainedUpdate(updateCommandGroups)
	//t.Fail()

}

func TestBulkChainedUpdateV6(t *testing.T) {
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	// executor, _ := elasticsearch.NewEsExecutor(context.Background(), []string{""})
	// bulkIngester := NewBulkIngester(executor)
	// typedIngester := NewTypedIngester(bulkIngester, "document")
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV6(typedIngester, 1)
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

	indexingWorker.bulkChainedUpdate(updateCommandGroups)
	//t.Fail()

}

func TestEsClient(t *testing.T) {
	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}
	response, err := es.Info().Do(context.Background())
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}

	t.Logf("Client: %s", elasticsearch.Version)
	t.Logf("Server: %s", response.Version.Int)
}

func TestCreateIndex(t *testing.T) {
	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}
	_, err = es.Indices.Create("myotherotherindex").Do(context.Background())
	if err != nil {
		t.Errorf("elasticsearchv8.C().PutIndex(): %s", err)
	}
}

func TestGetIndex(t *testing.T) {
	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1)

	indices, err := indexingWorker.getIndices(typedIngester.DocumentType)
	if err != nil {
		t.Errorf("Error getting indices: %s", err)
	}
	t.Log(indices)

}