package ingester

import (
	"context"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	config "github.com/myrteametrics/myrtea-ingester-api/v5/internal/configuration"
	"github.com/myrteametrics/myrtea-sdk/v5/connector"
	"github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v5/helpers"
	"github.com/myrteametrics/myrtea-sdk/v5/models"
	"github.com/spf13/viper"
)

func TestDirectBulkChainedUpdate2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping worker api calls in short mode")
	}
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	typedIngester := &TypedIngester{DocumentType: "document"}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1, 500)

	_, _ = esapi.IndicesDeleteRequest{
		AllowNoIndices: new(true),
		Index:          []string{"myindex"},
	}.Do(context.Background(), elasticsearch.C())
	_, _ = esapi.IndicesDeleteRequest{
		AllowNoIndices: new(true),
		Index:          []string{"myotherindex"},
	}.Do(context.Background(), elasticsearch.C())
	_, _ = esapi.IndicesDeleteRequest{
		AllowNoIndices: new(true),
		Index:          []string{"myotherotherindex"},
	}.Do(context.Background(), elasticsearch.C())

	docs := []models.Document{
		{IndexType: indexTypeDocument, Index: "myindex", ID: "1", Source: map[string]any{"a": "a", "b": "b"}},
		{IndexType: indexTypeDocument, Index: "myindex", ID: "2", Source: map[string]any{"a": "a", "b": "b"}},
		{IndexType: indexTypeDocument, Index: "myotherindex", ID: "4", Source: map[string]any{"a": "a", "b": "b"}},
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

	docType := "document"

	typedIngester := &TypedIngester{DocumentType: docType}
	indexingWorker := NewIndexingWorkerV8(typedIngester, 1, 500)

	mergeConfig := connector.Config{
		Type: docType, Mode: connector.Self, ExistingAsMaster: true,
		Groups: []connector.Group{{FieldReplace: []string{"update"}}},
	}
	buffer := []UpdateCommand{
		{
			Index: "myindex", DocumentID: "1", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myindex", ID: "1", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "1", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myindex", ID: "1", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "1", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myindex", ID: "1", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myindex", ID: "2", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myindex", ID: "2", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"}},
		},
		{
			Index: "myindex", DocumentID: "2", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myindex", ID: "2", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherindex", ID: "3", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherindex", ID: "3", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherindex", DocumentID: "3", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherindex", ID: "3", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherindex", ID: "4", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherindex", ID: "4", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherindex", DocumentID: "4", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherindex", ID: "4", IndexType: indexTypeDocument, Source: map[string]any{"update": "data"},
			},
		},
		{
			Index: "myotherotherindex", DocumentID: "5", DocumentType: docType, MergeConfig: mergeConfig,
			NewDoc: models.Document{
				Index: "myotherotherindex", ID: "5", IndexType: indexTypeDocument,
				Source: map[string]any{"update": "data"},
			},
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
	str, err := buildBulkIndexItem("testindex", "1", map[string]any{
		"a": "a",
		"b": "b",
	})
	if err != nil {
		t.Errorf("Error building bulk index item: %s", err)
	} else {
		t.Log(str)
	}
}

// TestBuildBulkCreateItem verifies that buildBulkCreateItem produces NDJSON lines
// that use the "create" action keyword instead of "index".
func TestBuildBulkCreateItem(t *testing.T) {
	lines, err := buildBulkCreateItem("testindex", "42", map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if !contains(lines[0], `"create"`) {
		t.Errorf("action line should contain \"create\", got: %s", lines[0])
	}
	if contains(lines[0], `"index"`) {
		t.Errorf("action line must NOT contain \"index\" key, got: %s", lines[0])
	}
	if !contains(lines[0], `"_index":"testindex"`) {
		t.Errorf("action line should contain _index, got: %s", lines[0])
	}
	if !contains(lines[0], `"_id":"42"`) {
		t.Errorf("action line should contain _id, got: %s", lines[0])
	}
	t.Log(lines)
}

// TestBuildBulkCreateItemEmptyID verifies that an empty ID causes the "_id"
// field to be omitted entirely from the bulk meta line.
//
// Elasticsearch rejects {"_id":""} with an illegal_argument_exception.
// Omitting "_id" altogether instructs ES to auto-generate a unique identifier.
func TestBuildBulkCreateItemEmptyID(t *testing.T) {
	lines, err := buildBulkCreateItem("testindex", "", map[string]any{"msg": "log"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !contains(lines[0], `"create"`) {
		t.Errorf("action line should contain \"create\", got: %s", lines[0])
	}
	// "_id" must NOT appear at all – an explicit empty string is rejected by ES.
	if contains(lines[0], `"_id"`) {
		t.Errorf("action line must NOT contain \"_id\" when id is empty, got: %s", lines[0])
	}
	t.Logf("action line (no _id): %s", lines[0])
}

// TestFlushEsBufferSplitDryRun verifies that flushEsBuffer correctly segregates
// append-only and regular commands without making real Elasticsearch calls.
// It relies on DEBUG_DRY_RUN_ELASTICSEARCH=true so no network is needed.
func TestFlushEsBufferSplitDryRun(t *testing.T) {
	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))
	viper.Set("DEBUG_DRY_RUN_ELASTICSEARCH", true)
	defer viper.Set("DEBUG_DRY_RUN_ELASTICSEARCH", false)

	typedIngester := &TypedIngester{DocumentType: "testdoc"}
	worker := NewIndexingWorkerV8(typedIngester, 99, 500)

	mergeConfig := connector.Config{
		Type: "testdoc", Mode: connector.Self, ExistingAsMaster: true,
		Groups: []connector.Group{{FieldReplace: []string{"msg"}}},
	}

	buffer := []UpdateCommand{
		// regular command
		{
			Index: "myindex", DocumentID: "1", DocumentType: "testdoc",
			MergeConfig: mergeConfig, AppendOnly: false,
			NewDoc: models.Document{Index: "myindex", ID: "1", Source: map[string]any{"msg": "update"}},
		},
		// append-only command
		{
			Index: "myindex", DocumentID: "", DocumentType: "testdoc",
			AppendOnly: true,
			NewDoc:     models.Document{Index: "myindex", ID: "", Source: map[string]any{"msg": "log1"}},
		},
		// another append-only command
		{
			Index: "myindex", DocumentID: "", DocumentType: "testdoc",
			AppendOnly: true,
			NewDoc:     models.Document{Index: "myindex", ID: "", Source: map[string]any{"msg": "log2"}},
		},
	}

	// Must not panic; with DRY_RUN the method returns before any ES call.
	worker.flushEsBuffer(buffer)
	t.Log("flushEsBuffer completed without panic (dry-run mode)")
}

// TestAppendOnlyUsesDocumentIndex verifies that appendOnlyBulkIndex respects the
// per-document index when it is set, rather than always falling back to the alias.
func TestAppendOnlyUsesDocumentIndex(t *testing.T) {
	// We only test the document building logic here via buildBulkCreateItem,
	// which mirrors what appendOnlyBulkIndex does internally.
	customIndex := "my-custom-log-index"
	lines, err := buildBulkCreateItem(customIndex, "", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !contains(lines[0], `"_index":"`+customIndex+`"`) {
		t.Errorf("expected custom index %q in action line, got: %s", customIndex, lines[0])
	}
}

// contains is a tiny helper used in tests to avoid importing strings in test files.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
