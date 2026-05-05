package ingester_test

// HTTP integration tests for the ingester-api.
//
// These tests send real HTTP requests to a running ingester-api instance.
// They are skipped automatically when running in short mode (`-short`).
//
// ─── How to run ────────────────────────────────────────────────────────────────
//
//   # All integration tests
//   go test -v -run TestHTTPIngest ./internal/ingester/
//
//   # AppendOnly=true only
//   go test -v -run TestHTTPIngestAppendOnly ./internal/ingester/
//
//   # AppendOnly=false only  (merge / regular update path)
//   go test -v -run TestHTTPIngestRegular ./internal/ingester/
//
//   # Both in sequence
//   go test -v -run TestHTTPIngestBoth ./internal/ingester/
//
//   # High-volume stress batch
//   go test -v -run TestHTTPIngestHighVolume ./internal/ingester/
//
// ─── Configuration ─────────────────────────────────────────────────────────────
//
// Edit the constants block below to match your running instance.

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

// ─── Test configuration ────────────────────────────────────────────────────────
// Adjust these values to match your running instance before running the tests.

const (
	// httpBaseURL is the base URL of the running ingester-api instance.
	httpBaseURL = "http://localhost:9001"

	// httpIngestPath is the path of the ingest endpoint.
	httpIngestPath = "/api/v1/ingester/data"

	// testDocType is the Elasticsearch document type used in every test.
	// Workers derive the alias names from this value:
	//   write alias  →  <INSTANCE_NAME>-<testDocType>-last
	//   read alias   →  <INSTANCE_NAME>-<testDocType>-patch
	testDocType = "test-integration"

	// testIndexName is the ACTUAL Elasticsearch index name used for
	// AppendOnly=false documents when ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true.
	// In that mode workers call mget on this index directly (no alias resolution).
	// Example: "myrtea-test-integration-last"
	testIndexName = "myrtea-ingester-test-integration-last"

	// testDocCount is the default number of random documents per test run.
	// Set to a higher value for stress tests.
	testDocCount = 20

	// testFixedIDPoolSize is the number of distinct document IDs used for
	// regular (AppendOnly=false) tests.  Re-using the same IDs across multiple
	// runs exercises the merge path (mget hit → merge → index).
	testFixedIDPoolSize = 10
)

// ─── Payload types (mirrors BulkIngestRequest / Document / connector.Config) ──

// bulkIngestPayload is the JSON payload sent to POST /ingester/data.
type bulkIngestPayload struct {
	UUID         string         `json:"uuid"`
	DocumentType string         `json:"documentType"`
	MergeConfig  []mergeConfig  `json:"merge,omitempty"`
	Docs         []testDocument `json:"docs"`
	AppendOnly   bool           `json:"appendOnly"`
}

// testDocument mirrors models.Document.
type testDocument struct {
	ID        string         `json:"id"`
	Index     string         `json:"index"`
	IndexType string         `json:"type"`
	Source    map[string]any `json:"source"`
}

// mergeConfig mirrors connector.Config (subset used in tests).
type mergeConfig struct {
	Mode             string       `json:"mode"`
	ExistingAsMaster bool         `json:"existingAsMaster"`
	Type             string       `json:"type,omitempty"`
	Groups           []mergeGroup `json:"groups,omitempty"`
}

// mergeGroup mirrors connector.Group (subset used in tests).
type mergeGroup struct {
	FieldReplace          []string `json:"fieldReplace,omitempty"`
	FieldReplaceIfMissing []string `json:"fieldReplaceIfMissing,omitempty"`
}

// ─── Default merge config ──────────────────────────────────────────────────────

// defaultMergeConfig returns a standard self-merge config that:
//   - treats the EXISTING document as master (its values are preserved by default)
//   - fills in name / category / value / score / tags from the incoming doc only
//     when those fields are missing on the existing one
func defaultMergeConfig() mergeConfig {
	return mergeConfig{
		Mode:             "self",
		ExistingAsMaster: true,
		Type:             testDocType,
		Groups: []mergeGroup{
			{
				// Replace the tracked fields unconditionally on every update.
				FieldReplace: []string{"value", "score", "updatedAt"},
				// Fill in immutable fields only when not already present.
				FieldReplaceIfMissing: []string{"name", "category", "tags", "createdAt"},
			},
		},
	}
}

// ─── Random document generators ───────────────────────────────────────────────

var (
	categories = []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	tags       = []string{"hot", "cold", "new", "legacy", "critical", "stable"}

	// fixedIDs is a small pool of IDs reused across regular (AppendOnly=false) runs
	// so that successive test runs exercise the merge path.
	fixedIDs = func() []string {
		ids := make([]string, testFixedIDPoolSize)
		for i := range ids {
			ids[i] = fmt.Sprintf("test-doc-%04d", i+1)
		}
		return ids
	}()
)

// randomSource generates a realistic document payload.
func randomSource(rng *rand.Rand) map[string]any {
	return map[string]any{
		"name":      fmt.Sprintf("item-%s", randomHex(rng, 4)),
		"category":  categories[rng.Intn(len(categories))],
		"value":     rng.Float64() * 1000,
		"score":     rng.Intn(100),
		"tags":      randomTagSubset(rng),
		"createdAt": time.Now().UTC().Add(-time.Duration(rng.Intn(3600)) * time.Second).Format(time.RFC3339),
		"updatedAt": time.Now().UTC().Format(time.RFC3339),
	}
}

func randomHex(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	rng.Read(b)
	return fmt.Sprintf("%x", b)
}

func randomTagSubset(rng *rand.Rand) []string {
	n := 1 + rng.Intn(3)
	perm := rng.Perm(len(tags))
	result := make([]string, n)
	for i := range result {
		result[i] = tags[perm[i]]
	}
	return result
}

// ─── Document builders ─────────────────────────────────────────────────────────

// buildAppendOnlyDocs generates n documents for AppendOnly=true.
// IDs are left empty so Elasticsearch auto-generates them.
// The index can be set explicitly or left empty to fall back to the alias.
func buildAppendOnlyDocs(rng *rand.Rand, n int, index string) []testDocument {
	docs := make([]testDocument, n)
	for i := range docs {
		docs[i] = testDocument{
			// Empty ID → ES auto-generates; avoids 409 conflicts on re-runs.
			ID:        "",
			Index:     index,
			IndexType: "document",
			Source:    randomSource(rng),
		}
	}
	return docs
}

// buildRegularDocs generates n documents for AppendOnly=false.
// IDs are drawn from the fixed pool so that consecutive runs merge on the
// same documents (exercising the mget → merge path).
func buildRegularDocs(rng *rand.Rand, n int, index string) []testDocument {
	docs := make([]testDocument, n)
	for i := range docs {
		docs[i] = testDocument{
			ID:        fixedIDs[i%testFixedIDPoolSize],
			Index:     index,
			IndexType: "document",
			Source:    randomSource(rng),
		}
	}
	return docs
}

// ─── HTTP helpers ──────────────────────────────────────────────────────────────

// sendIngestRequest serialises payload as JSON and POSTs it to the ingest endpoint.
// It asserts that the server returns HTTP 200.
func sendIngestRequest(t *testing.T, payload bulkIngestPayload) {
	t.Helper()

	body, err := jsoniter.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	url := httpBaseURL + httpIngestPath
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		t.Fatalf("POST %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	t.Logf("POST %s → HTTP %d  (uuid=%s, docs=%d, appendOnly=%v)",
		url, resp.StatusCode, payload.UUID, len(payload.Docs), payload.AppendOnly)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected HTTP 200, got %d", resp.StatusCode)
	}
}

// newRng returns a seeded random source.  Using time-based seed ensures each
// test run generates a distinct set of documents.
func newRng() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

// TestHTTPIngestAppendOnly sends a batch of documents with AppendOnly=true.
//
// Each document gets an auto-generated ES ID so there are no merge conflicts.
// This exercises the appendOnlyBulkIndex → bulkCreate path.
//
//	go test -v -run TestHTTPIngestAppendOnly ./internal/ingester/
func TestHTTPIngestAppendOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires a running ingester-api at " + httpBaseURL)
	}

	rng := newRng()

	payload := bulkIngestPayload{
		UUID:         uuid.New().String(),
		DocumentType: testDocType,
		AppendOnly:   true,
		// MergeConfig is intentionally omitted – not needed for append-only.
		Docs: buildAppendOnlyDocs(rng, testDocCount, testIndexName),
	}

	t.Logf("Sending %d append-only documents to documentType=%q index=%q",
		len(payload.Docs), testDocType, testIndexName)
	sendIngestRequest(t, payload)
}

// TestHTTPIngestRegular sends a batch of documents with AppendOnly=false.
//
// Documents are sent with a self-merge config and a fixed ID pool so that
// re-running the test exercises the "mget hit → merge → index" path.
// Requires ELASTICSEARCH_DIRECT_MULTI_GET_MODE=true to work with testIndexName.
//
//	go test -v -run TestHTTPIngestRegular ./internal/ingester/
func TestHTTPIngestRegular(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires a running ingester-api at " + httpBaseURL)
	}

	rng := newRng()

	payload := bulkIngestPayload{
		UUID:         uuid.New().String(),
		DocumentType: testDocType,
		AppendOnly:   false,
		MergeConfig:  []mergeConfig{defaultMergeConfig()},
		Docs:         buildRegularDocs(rng, testDocCount, testIndexName),
	}

	t.Logf("Sending %d regular documents (AppendOnly=false) to documentType=%q index=%q",
		len(payload.Docs), testDocType, testIndexName)
	sendIngestRequest(t, payload)
}

// TestHTTPIngestBoth sends one regular batch followed by one append-only batch.
// Useful for a quick end-to-end sanity check of both paths.
//
//	go test -v -run TestHTTPIngestBoth ./internal/ingester/
func TestHTTPIngestBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires a running ingester-api at " + httpBaseURL)
	}

	t.Run("regular", func(t *testing.T) {
		rng := newRng()
		payload := bulkIngestPayload{
			UUID:         uuid.New().String(),
			DocumentType: testDocType,
			AppendOnly:   false,
			MergeConfig:  []mergeConfig{defaultMergeConfig()},
			Docs:         buildRegularDocs(rng, testDocCount, testIndexName),
		}
		t.Logf("Sending %d regular documents", len(payload.Docs))
		sendIngestRequest(t, payload)
	})

	t.Run("append_only", func(t *testing.T) {
		rng := newRng()
		payload := bulkIngestPayload{
			UUID:         uuid.New().String(),
			DocumentType: testDocType,
			AppendOnly:   true,
			Docs:         buildAppendOnlyDocs(rng, testDocCount, testIndexName),
		}
		t.Logf("Sending %d append-only documents", len(payload.Docs))
		sendIngestRequest(t, payload)
	})
}

// TestHTTPIngestHighVolume sends multiple large batches back-to-back to stress
// the worker buffer and flush logic.
//
// Adjust volumeDocCount and volumeBatches to your needs.
//
//	go test -v -run TestHTTPIngestHighVolume ./internal/ingester/
func TestHTTPIngestHighVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires a running ingester-api at " + httpBaseURL)
	}

	const (
		volumeDocCount = 200 // documents per batch
		volumeBatches  = 5   // number of successive batches
	)

	rng := newRng()

	for batch := range volumeBatches {
		appendOnly := batch%2 == 0 // alternate modes for variety

		var docs []testDocument
		if appendOnly {
			docs = buildAppendOnlyDocs(rng, volumeDocCount, testIndexName)
		} else {
			docs = buildRegularDocs(rng, volumeDocCount, testIndexName)
		}

		var mc []mergeConfig
		if !appendOnly {
			mc = []mergeConfig{defaultMergeConfig()}
		}

		payload := bulkIngestPayload{
			UUID:         uuid.New().String(),
			DocumentType: testDocType,
			AppendOnly:   appendOnly,
			MergeConfig:  mc,
			Docs:         docs,
		}

		t.Logf("Batch %d/%d – appendOnly=%v docs=%d", batch+1, volumeBatches, appendOnly, len(docs))
		sendIngestRequest(t, payload)

		// Small pause to avoid saturating the worker channel in unit test mode.
		time.Sleep(50 * time.Millisecond)
	}
}

// TestHTTPIngestCustom is a template you can edit freely to send a one-off
// payload with specific fields, IDs, or merge config for manual debugging.
//
//	go test -v -run TestHTTPIngestCustom ./internal/ingester/
func TestHTTPIngestCustom(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires a running ingester-api at " + httpBaseURL)
	}

	// ── Edit below this line ──────────────────────────────────────────────────
	appendOnly := true // ← flip to false for regular mode
	docCount := 5

	rng := newRng()

	var docs []testDocument
	var mc []mergeConfig
	if appendOnly {
		docs = buildAppendOnlyDocs(rng, docCount, testIndexName)
	} else {
		docs = buildRegularDocs(rng, docCount, testIndexName)
		mc = []mergeConfig{defaultMergeConfig()}
	}

	// You can also hardcode specific documents:
	//   docs = []testDocument{
	//       {ID: "my-specific-id", Index: testIndexName, IndexType: "document",
	//        Source: map[string]any{"name": "test", "value": 42}},
	//   }

	payload := bulkIngestPayload{
		UUID:         uuid.New().String(),
		DocumentType: testDocType,
		AppendOnly:   appendOnly,
		MergeConfig:  mc,
		Docs:         docs,
	}

	// Print the payload for inspection.
	raw, _ := jsoniter.MarshalIndent(payload, "", "  ")
	t.Logf("Sending payload:\n%s", raw)

	sendIngestRequest(t, payload)
}
