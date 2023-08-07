package ingester

import (
	"context"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
	config "github.com/myrteametrics/myrtea-ingester-api/v5/internals/configuration"
	"github.com/myrteametrics/myrtea-sdk/v4/connector"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearchv8"
	"github.com/myrteametrics/myrtea-sdk/v4/helpers"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func testV8Setup(t *testing.T, initEsClient bool) {
	t.Helper()
	helpers.InitLogger(false)
	helpers.InitializeConfig(config.GetAllowedConfigKey(), config.ConfigName, "../../config", config.EnvPrefix)

	if initEsClient {
		err := elasticsearchv8.ReplaceGlobals(
			elasticsearch.Config{ //nolint:exhaustruct
				Addresses:     []string{"http://localhost:9200"},
				EnableMetrics: true,
			})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
}

func testV8buildIngester(t *testing.T) *IndexingWorkerV8 {
	t.Helper()

	typedIngester := &TypedIngester{
		Uuid:                          uuid.New(),
		bulkIngester:                  nil,
		DocumentType:                  "document",
		Data:                          make(chan *IngestRequest, viper.GetInt("TYPEDINGESTER_QUEUE_BUFFER_SIZE")),
		Workers:                       make(map[int]IndexingWorker),
		maxWorkers:                    viper.GetInt("INGESTER_MAXIMUM_WORKERS"),
		metricTypedIngesterQueueGauge: _metricTypedIngesterQueueGauge.With("typedingester", "document"),
	}

	indexingWorker := NewIndexingWorkerV8(typedIngester, 1)

	return indexingWorker
}

func TestV8DirectMultiGetDocs(t *testing.T) {
	testV8Setup(t, true)
	indexingWorker := testV8buildIngester(t)

	res, err := esapi.IndicesDeleteRequest{ //nolint:exhaustruct
		AllowNoIndices: esapi.BoolPtr(true),
		Index:          []string{"myindex"},
	}.Do(context.Background(), elasticsearchv8.C())
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	_ = res

	docs := []models.Document{
		{IndexType: "document", Index: "myindex", ID: "1", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myindex", ID: "2", Source: map[string]interface{}{"a": "a", "b": "b"}},
		{IndexType: "document", Index: "myindex", ID: "3", Source: map[string]interface{}{"a": "a", "b": "b"}},
	}

	err = indexingWorker.bulkIndex(docs)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	resItems, err := indexingWorker.multiGetFindRefDocs("myindex", []GetQuery{
		{ID: "1", DocumentType: "document"},
		{ID: "999", DocumentType: "document"},
	})
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	t.Log(resItems)

	getdocs, err := indexingWorker.multiGetFindRefDocsFull([]string{"myindex"}, []GetQuery{
		{ID: "1", DocumentType: "document"},
		{ID: "999", DocumentType: "document"},
	})
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	t.Log(getdocs)
}

func TestV8ApplyMerges(t *testing.T) { //nolint:funlen
	testV8Setup(t, false)
	indexingWorker := testV8buildIngester(t)

	type testCase struct {
		commands [][]UpdateCommand
		refDocs  []models.Document
		expected []models.Document
		err      error
	}

	mergeConfig := connector.Config{
		Mode:             connector.Self,
		ExistingAsMaster: true,
		Type:             "document",
		LinkKey:          "",
		Groups: []connector.Group{
			{FieldReplaceIfMissing: []string{"y", "z"}},
		},
	}

	testCases := map[string]testCase{
		"two_docs_one_ref": {
			commands: [][]UpdateCommand{
				{
					NewUpdateCommand("", "1", "document",
						models.Document{ID: "1", Index: "", IndexType: "document", Source: map[string]interface{}{"y": "y"}},
						mergeConfig,
					),
					NewUpdateCommand("", "1", "document",
						models.Document{ID: "1", Index: "", IndexType: "document", Source: map[string]interface{}{"z": "z"}},
						mergeConfig,
					),
				},
			},
			refDocs: []models.Document{
				{
					ID: "1", Index: "myindex-existing", IndexType: "document",
					Source: map[string]interface{}{"f": "f"},
				},
			},
			expected: []models.Document{
				{
					ID: "1", Index: "myindex-existing", IndexType: "document",
					Source: map[string]interface{}{"f": "f", "y": "y", "z": "z"},
				},
			},
			err: nil,
		},
		"two_docs_missing_ref": {
			commands: [][]UpdateCommand{
				{
					NewUpdateCommand("myindex", "1", "document",
						models.Document{ID: "1", Index: "", IndexType: "document", Source: map[string]interface{}{"y": "y"}},
						mergeConfig,
					),
					NewUpdateCommand("myindex", "1", "document",
						models.Document{ID: "1", Index: "", IndexType: "document", Source: map[string]interface{}{"z": "z"}},
						mergeConfig,
					),
				},
			},
			refDocs: []models.Document{
				{
					ID: "", Index: "", IndexType: "",
					Source: map[string]interface{}{},
				},
			},
			expected: []models.Document{
				{
					ID: "1", Index: "myrtea-document-current", IndexType: "document",
					Source: map[string]interface{}{"y": "y", "z": "z"},
				},
			},
			err: nil,
		},
	}

	for name, testCase := range testCases {
		t.Log(name)

		out, err := indexingWorker.applyMerges(testCase.commands, testCase.refDocs)
		if err != nil {
			t.Error(err)
			t.Fail()
		}

		assert.Equal(t, out, testCase.expected)
	}
}
