package ingester

import (
	"hash/fnv"
	"time"

	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/merge"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// In case of "live" or "hot" workers number change :
// * Stop every injections
// * Send a flush order to every worker
// * (wait until done)
// * Change workers count
// * Re-allow injections to workers

// TypedIngester is a component which process IngestRequest
// It generates UpdateCommand which are processed by the attached IndexingWorker's
type TypedIngester struct {
	bulkIngester *BulkIngester
	DocumentType string
	Data         chan *IngestRequest
	Workers      map[int]*IndexingWorker
	maxWorkers   int
}

// NewTypedIngester returns a pointer to a new TypedIngester instance
func NewTypedIngester(bulkIngester *BulkIngester, documentType string) *TypedIngester {
	ingester := TypedIngester{}
	ingester.bulkIngester = bulkIngester
	ingester.DocumentType = documentType
	ingester.Data = make(chan *IngestRequest)
	ingester.Workers = make(map[int]*IndexingWorker)
	ingester.maxWorkers = viper.GetInt("INGESTER_MAXIMUM_WORKERS")
	for i := 0; i < ingester.maxWorkers; i++ {
		worker := NewIndexingWorker(&ingester, i)
		ingester.Workers[i] = worker
		go worker.Run()
		time.Sleep(10 * time.Millisecond) // goroutine warm-up
	}
	return &ingester
}

// Run is the main routine of a TypeIngester instance
// In case of Mode == SELF
// * The in-memory cache is filled with new informations
// * An update command is send to the dedicated indexer
//
// In case of Mode == ENRICH_FROM (Which might be the same at last ?)
// * An update command is send to the dedicated indexer
//
// In case of Mode == ENRICH_TO (Which might be the same at last ?)
// * A dedicated "relation cache" is queried to find all the object which must be updated
// * One or multiple update command are sent to the dedicated indexer
//
func (ingester *TypedIngester) Run() {
	zap.L().Info("Starting TypedIngester",
		zap.String("documentType", ingester.DocumentType),
	)

	for {
		select {
		case ir := <-ingester.Data:
			zap.L().Debug("Receive IngestRequest",
				zap.String("IngesterType", ingester.DocumentType),
				zap.Any("IngestRequest", ir),
			)

			switch ir.MergeConfig.Mode {
			case merge.Self:
				if ir.DocumentType == "budget" {
					source := ir.Doc.Source.(map[string]interface{})
					ingester.bulkIngester.Cache.AddToSlice(source["project-id"].(string), ir.Doc.ID)
					ingester.bulkIngester.Cache.Dump()
				}
				fallthrough //!\\ Keep this

			case merge.EnrichFrom:
				workerID := getWorker(ir.Doc.ID, ingester.maxWorkers)
				updateCommand := NewUpdateCommand(ir.Doc.ID, ir.DocumentType, ir.Doc, ir.MergeConfig)
				zap.L().Debug("Send UpdateCommand",
					zap.String("IngesterType", ingester.DocumentType),
					zap.Int("WorkerID", workerID),
					zap.Any("updateCommand", updateCommand),
				)
				ingester.Workers[workerID].Data <- updateCommand

			case merge.EnrichTo:
				// Request In-memory cache (native go map, Redis, etc.)
				// TODO: Configurable and selectable cache for multiple "relations" types (stored in a map)
				cachedData, found := ingester.bulkIngester.Cache.Get(ir.Doc.ID)
				if found {
					ids := cachedData.([]interface{})
					for _, id := range ids {
						workerID := getWorker(id.(string), ingester.maxWorkers)
						updateCommand := NewUpdateCommand(id.(string), ir.MergeConfig.Type, ir.Doc, ir.MergeConfig)
						zap.L().Debug("Send UpdateCommand",
							zap.String("IngesterType", ingester.DocumentType),
							zap.Int("WorkerID", workerID),
							zap.Any("updateCommand", updateCommand),
						)
						ingester.Workers[workerID].Data <- updateCommand
					}
				} else {
					zap.L().Debug("No cache data found, request skipped",
						zap.String("IngesterType", ingester.DocumentType),
						zap.String("key", ir.Doc.ID),
					)
				}

			default:
				zap.L().Error("Unknown merge mode",
					zap.String("IngesterType", ingester.DocumentType),
					zap.String("mode", ir.MergeConfig.Mode.String()),
				)
			}
		}
	}
}

// getWorker returns a workerID based on a UUID hash
func getWorker(uuid string, maxWorker int) int {
	hash := hash(uuid)
	return int(hash % uint32(maxWorker))
}

// hash hash a string (for potential routing)
func hash(str string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(str))
	return h.Sum32()
}
