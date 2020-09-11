package ingester

import (
	"context"
	"fmt"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/merge"
	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v4/index"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// IndexingWorker is the unit of processing which can be started in parallel for elasticsearch ingestion
type IndexingWorker struct {
	TypedIngester *TypedIngester
	ID            int
	Data          chan *UpdateCommand
	Client        *elasticsearch.EsExecutor
}

// NewIndexingWorker returns a new IndexingWorker
func NewIndexingWorker(typedIngester *TypedIngester, id int) *IndexingWorker {
	worker := IndexingWorker{TypedIngester: typedIngester, ID: id}
	worker.Data = make(chan *UpdateCommand)

	zap.L().Info("Initialize Elasticsearch client", zap.String("status", "in_progress"))
	var err error
	worker.Client, err = elasticsearch.NewEsExecutor(context.Background(), viper.GetStringSlice("ELASTICSEARCH_URLS"))
	if err != nil {
		zap.L().Error("Elasticsearch client initialization", zap.Error(err))
	} else {
		zap.L().Info("Initialize Elasticsearch client", zap.String("status", "done"))
	}
	return &worker
}

// Run start a worker
func (worker *IndexingWorker) Run() {
	zap.L().Info("Starting IndexingWorker",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
	)

	bufferLength := viper.GetInt("WORKER_MAXIMUM_BUFFER_SIZE")
	buffer := make([]*UpdateCommand, 0)

	forceFlushTimeout := viper.GetInt("WORKER_FORCE_FLUSH_TIMEOUT_SEC")
	forceFlush := worker.resetForceFlush(forceFlushTimeout)

	for {
		select {

		// Send indexing bulk (when buffer is full or on timeout)
		case <-forceFlush:
			zap.L().Debug("Try on after timeout reached", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID), zap.Int("Timeout", forceFlushTimeout))
			if len(buffer) > 0 {
				zap.L().Info("Flushing on timeout reached", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
					zap.Int("WorkerID", worker.ID), zap.Int("Messages", len(buffer)), zap.Int("Timeout", forceFlushTimeout))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
			}
			forceFlush = worker.resetForceFlush(forceFlushTimeout)

		// Build indexing bulk
		case uc := <-worker.Data:
			zap.L().Debug("Receive UpdateCommand", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
				zap.Int("WorkerID", worker.ID), zap.Any("UpdateCommand", uc))
			buffer = append(buffer, uc)

			if len(buffer) >= bufferLength {
				zap.L().Info("Try flushing on full buffer", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
					zap.Int("WorkerID", worker.ID), zap.Int("Messages", bufferLength))
				worker.flushEsBuffer(buffer)
				buffer = buffer[:0]
				forceFlush = worker.resetForceFlush(forceFlushTimeout)
			}
		}
	}
}

func (worker *IndexingWorker) resetForceFlush(sec int) <-chan time.Time {
	return time.After(time.Duration(sec) * time.Second)
}

func (worker *IndexingWorker) flushEsBuffer(buffer []*UpdateCommand) {
	if len(buffer) == 0 {
		return
	}

	m := make(map[string][]*UpdateCommand)
	for _, uc := range buffer {
		if m[uc.DocumentID] != nil {
			m[uc.DocumentID] = append(m[uc.DocumentID], uc)
		} else {
			m[uc.DocumentID] = []*UpdateCommand{uc}
		}
	}

	sl := make([][]*UpdateCommand, 0)
	for _, v := range m {
		sl = append(sl, v)
	}

	for key, entry := range m {
		zap.L().Debug("flushEsBuffer", zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID), zap.String("key", key), zap.Int("len(entry)", len(entry)))
	}

	if viper.GetBool("DEBUG_DRY_RUN_ELASTICSEARCH") {
		return
	}
	worker.BulkChainedUpdate(sl)
}

// BulkChainedUpdate process multiple groups of UpdateCommand
// It execute sequentialy every single UpdateCommand on a specific "source" document, for each group of commands
func (worker *IndexingWorker) BulkChainedUpdate(documents [][]*UpdateCommand) {

	zap.L().Debug("BulkChainUpdate",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.Any("documents", documents),
	)

	docs := make([]GetQuery, 0)
	secondary := make([]GetQuery, 0)
	secondaryM := make(map[string]bool, 0) // Alternative to Set[struct{}] assertion...
	for _, commands := range documents {
		docs = append(docs, GetQuery{DocumentType: commands[0].DocumentType, ID: commands[0].DocumentID})
		for _, command := range commands {
			if command.MergeConfig.Mode == merge.EnrichFrom {
				key := command.MergeConfig.LinkKey
				source := command.NewDoc.Source.(map[string]interface{})

				mapKey := command.MergeConfig.Type + "#" + source[key].(string)
				if !secondaryM[mapKey] {
					secondary = append(secondary, GetQuery{DocumentType: command.MergeConfig.Type, ID: source[key].(string)})
					secondaryM[mapKey] = true
				}
			}
		}
	}

	zap.L().Debug("Main Call",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.Any("docs", docs),
	)
	// zap.L().Debug("Secondary Call",
	// 	zap.String("TypedIngester", worker.TypedIngester.DocumentType),
	// 	zap.Int("WorkerID", worker.ID),
	// 	zap.Any("secondary", secondary),
	// )

	// FIXME: replace with proper method and SDK Depth
	alias := buildAliasName(docs[0].DocumentType, index.Patch)
	indices, err := worker.Client.GetIndicesByAlias(context.Background(), alias)
	if err != nil {
		zap.L().Error("GetIndicesByAlias", zap.Error(err), zap.String("alias", alias))
	}

	// TODO: parrallelism of multiple bulk get ?
	// Or chain GET on missing results only (instead of full set)
	refDocs := make([]*models.Document, 0)
	for _, index := range indices {

		d := make([]*models.Document, 0)
		for _, doc := range docs {
			d = append(d, models.NewDocument(doc.ID, index, "document", nil))
		}
		response, err := worker.Client.MultiGet(context.Background(), d)

		zap.L().Debug("MULTIGET", zap.Any("d", d), zap.Any("response", response))

		if err != nil || response.Docs == nil || len(response.Docs) == 0 {
			zap.L().Error("MultiGet (self)", zap.Error(err))
			continue
		}
		for i, d := range response.Docs {
			data, err := jsoniter.Marshal(d.Source)
			if err != nil {
				zap.L().Error("UPDATE MULTIGET unmarshal", zap.Error(err))
			}
			var source map[string]interface{}
			err = jsoniter.Unmarshal(data, &source)
			if err != nil {
				zap.L().Error("UPDATE MULTIGET unmarshal", zap.Error(err))
			}

			if len(refDocs) > i && refDocs[i] == nil {
				if d.Found {
					refDocs[i] = models.NewDocument(d.Id, d.Index, d.Type, source)
				}
			} else {
				if d.Found {
					refDocs = append(refDocs, models.NewDocument(d.Id, d.Index, d.Type, source))
				} else {
					refDocs = append(refDocs, nil)
				}
			}
		}
	}

	// TODO: Replace code in executor instead of dirty conversion
	s := make([]*models.Document, 0)
	for _, sec := range secondary {
		s = append(s, sec.convertToExecutor())
	}
	// END TODO: Replace code in executor instead of dirty conversion

	// TODO: Replace by an in-memory cache ?
	var enrichDocs = make([]*models.Document, 0)
	if len(s) > 0 {
		// TODO: Replace by in-memory cache OR MultiSearch()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		response2, err := worker.Client.MultiGet(ctx, s)
		if err != nil {
			zap.L().Error("MultiSearch (enrich_to)", zap.Error(err))
		}
		zap.L().Debug("response2", zap.Any("s", s), zap.Any("response2", response2))

		for _, d := range response2.Docs {
			if d.Found == false {
				continue
			}
			data, err := jsoniter.Marshal(d.Source)
			if err != nil {
				zap.L().Error("UPDATE MULTIGET unmarshal", zap.Error(err))
			}
			var source map[string]interface{}
			err = jsoniter.Unmarshal(data, &source)
			if err != nil {
				zap.L().Error("UPDATE MULTIGET unmarshal", zap.Error(err))
			}
			doc := models.NewDocument(d.Id, d.Index, d.Type, source)

			zap.L().Debug("found", zap.Any("d", d), zap.Any("doc", doc))
			enrichDocs = append(enrichDocs, doc)
		}
	}

	var push = make([]*models.Document, 0)

	var i int
	for _, commands := range documents {
		var doc *models.Document
		//if response.Responses[i] != nil && response.Responses[i].Hits != nil && len(response.Responses[i].Hits.Hits) > 0 {
		//	d := response.Responses[i].Hits.Hits[0]
		// if response.Docs != nil && len(response.Docs) > 0 && response.Docs[i] != nil {
		// 	d := response.Docs[i]
		// 	data, err := jsoniter.Marshal(d.Source)
		// 	if err != nil {
		// 		zap.L().Error("UPDATE MULTIGET unmarshal", zap.Error(err))
		// 	}
		// 	var source map[string]interface{}
		// 	err = jsoniter.Unmarshal(data, &source)
		// 	if err != nil {
		// 		zap.L().Error("UPDATE MULTIGET unmarshal", zap.Error(err))
		// 	}
		// 	doc = models.NewDocument(d.Id, d.Index, d.Type, source)
		// }

		if len(refDocs) > i {
			doc = refDocs[i]
		}

		// Index setup should probably not be here (be before in the indexing chain)
		for _, command := range commands {
			if command.NewDoc.Index == "" {
				command.NewDoc.Index = buildAliasName(command.DocumentType, index.Last)
			}
			doc = ApplyMerge(doc, command, enrichDocs)
		}
		doc.IndexType = "document"
		push = append(push, doc)
		i++ // synchronise map iteration with reponse.Docs
	}

	zap.L().Debug("Final document output",
		zap.String("TypedIngester", worker.TypedIngester.DocumentType),
		zap.Int("WorkerID", worker.ID),
		zap.Any("PushDocs", push),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	bulkResponse, err := worker.Client.BulkIndex(ctx, push)
	if err != nil {
		zap.L().Error("BulkIndex response",
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.Any("documents", documents),
			zap.Error(err))
		return
	}
	if bulkResponse != nil && len(bulkResponse.Failed()) > 0 {
		zap.L().Error("Error during bulkIndex",
			zap.String("TypedIngester", worker.TypedIngester.DocumentType),
			zap.Int("WorkerID", worker.ID),
			zap.Any("documents", documents),
			zap.Any("bulkResponse.Failed()", bulkResponse.Failed()))
	}
	zap.L().Debug("BulkIndex response", zap.Any("bulkResponse", bulkResponse))
	//return bulkResponse, err
}

// ApplyMerge execute a merge based on a specific UpdateCommand
func ApplyMerge(doc *models.Document, command *UpdateCommand, secondary []*models.Document) *models.Document {
	// Important : "doc" is always the output document

	zap.L().Debug("ApplyMerge",
		zap.String("mergeMode", command.MergeConfig.Mode.String()),
		zap.Any("doc", doc),
		zap.Any("command", command),
		zap.Any("secondary", secondary),
	)

	switch command.MergeConfig.Mode {
	case merge.Self:
		// COMMAND.NEWDOC enriched with DOC (pointer swap !) with config COMMAND.MERGECONFIG
		// The new pushed document become the new "reference" (and is enriched by the data of an existing one)
		output := command.MergeConfig.Apply(command.NewDoc, doc)
		zap.L().Debug("ApplyMergeResult", zap.Any("output", output))
		return output

	case merge.EnrichFrom:
		// COMMAND.NEWDOC enriched by SECONDARY[KEY] with config COMMAND.MERGECONFIG
		for _, sec := range secondary {
			if sec == nil {
				continue
			}
			key := command.MergeConfig.LinkKey
			source := command.NewDoc.Source.(map[string]interface{})
			if sec.IndexType == command.MergeConfig.Type && sec.ID == source[key] {
				command.MergeConfig.Apply(doc, sec)
				break // TODO: what about multiple external document enriching a single one ?
			}
		}
		zap.L().Debug("ApplyMergeResult", zap.Any("doc", doc))
		return doc

	case merge.EnrichTo:
		// DOC enriched WITH COMMAND.NEWDOC (NO pointer swap !) with config COMMAND.MERGECONFIG
		// The old existing document stay the reference (and is enriched with the data of a new one)
		command.MergeConfig.Apply(doc, command.NewDoc)
		zap.L().Debug("ApplyMergeResult", zap.Any("doc", doc))
		return doc
	}
	return nil
}

// GetQuery ...
type GetQuery struct {
	DocumentType string
	ID           string
}

func (getQuery *GetQuery) convertToExecutor() *models.Document {
	alias := buildAliasName(getQuery.DocumentType, index.All)
	return models.NewDocument(getQuery.ID, alias, "document", nil)
}

func buildAliasName(documentType string, depth index.Depth) string {
	access := fmt.Sprintf("%s-%s-%s",
		viper.GetString("INSTANCE_NAME"),
		documentType,
		strings.ToLower(depth.String()),
	)
	return access
}
