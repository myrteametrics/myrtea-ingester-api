package ingester

import (
	"github.com/google/uuid"
	"github.com/myrteametrics/myrtea-sdk/v4/connector"
	"github.com/myrteametrics/myrtea-sdk/v4/expression"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/spf13/viper"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var testMergeConfig = connector.Config{
	Mode:             connector.Self,
	Type:             "test",
	ExistingAsMaster: true,
	Groups: []connector.Group{
		{
			FieldMath: []connector.FieldMath{
				{
					Expression:  `length(Existing.test_data_6) == 1 && Existing.test_data_4 == false && contains(["a", "b"], New.test_data_6)`,
					OutputField: "anomaly_part_event_sec_inter",
				},
			},
		},
		{
			FieldReplace: []string{
				"test_data_5", // date
			},
		},
		{
			Condition: "Existing.test_data_4 == false",
			FieldReplace: []string{
				"test_data_4", // bool
			},
		},
		{
			Condition: "datemillis(New.update_date_time) >= datemillis(Existing.update_date_time)",
			FieldReplace: []string{
				"test_data_1",
				"test_data_2",
				"test_data_3",
			},
		},
		{
			FieldReplaceIfMissing: []string{
				"test_data_1",
				"test_data_2",
				"test_data_3",
			},
		},
	},
}

func generateRandomDoc(uuidStr string) models.Document {
	source := map[string]interface{}{
		"test_data_1":      getRandomKeyword(),
		"test_data_4":      randBool(),
		"test_data_5":      randDate(),
		"test_data_6":      getTestData6(),
		"update_date_time": randDate(),
	}

	if rand.Float64() < 0.5 {
		source["test_data_3"] = getRandomKeyword()
	}

	if rand.Float64() < 0.5 {
		source["test_data_2"] = getRandomKeyword()
	}

	return models.Document{
		ID:        uuidStr,
		Index:     "test",
		IndexType: "_doc",
		Source:    source,
	}
}

// generateSingleUpdateCommand generates a single UpdateCommand instance based on the provided data
func generateSingleUpdateCommand() UpdateCommand {
	doc := generateRandomDoc(uuid.New().String())
	return UpdateCommand{
		DocumentID:   doc.ID,
		DocumentType: "test",
		NewDoc:       doc,
		MergeConfig:  testMergeConfig,
		Index:        "testidx",
	}
}

// Helper function to generate a random string
func randStringBytes(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// Helper function to generate a random boolean
func randBool() bool {
	return rand.Intn(2) == 0
}

// Helper function to generate a random date
func randDate() time.Time {
	// Generate a random date within the last year
	min := time.Now().AddDate(0, 0, -365).Unix()
	max := time.Now().Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min

	return time.Unix(sec, 0)
}

func duplicateListRandomButSameUUID(list []UpdateCommand) []models.Document {
	duplicateList := make([]models.Document, len(list))
	for i, v := range list {
		duplicateList[i] = generateRandomDoc(v.NewDoc.ID)
	}
	return duplicateList
}

func BenchmarkApplyMergesV2(b *testing.B) {
	viper.Set("APPLY_MERGE_WORKER_COUNT", 4)
	worker := &IndexingWorkerV8{}
	updateCommandGroups := make([][]UpdateCommand, 0)
	buffer := make([]UpdateCommand, 0)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < b.N; i++ {
		buffer = append(buffer, generateSingleUpdateCommand())
	}

	refDocs := duplicateListRandomButSameUUID(buffer)

	updateCommandGroupsMap := make(map[string][]UpdateCommand)
	for _, uc := range buffer {
		if updateCommandGroupsMap[uc.DocumentID] != nil {
			updateCommandGroupsMap[uc.DocumentID] = append(updateCommandGroupsMap[uc.DocumentID], uc)
		} else {
			updateCommandGroupsMap[uc.DocumentID] = []UpdateCommand{uc}
		}
	}

	for _, v := range updateCommandGroupsMap {
		updateCommandGroups = append(updateCommandGroups, v)
	}

	b.ResetTimer()

	_, err := worker.applyMergesV2(updateCommandGroups, refDocs)
	if err != nil {
		b.Error(err)
	}

}

func BenchmarkApplyMergesV3(b *testing.B) {
	viper.Set("APPLY_MERGE_WORKER_COUNT", 8)
	worker := &IndexingWorkerV8{}
	updateCommandGroups := make([][]UpdateCommand, 0)
	buffer := make([]UpdateCommand, 0)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < b.N; i++ {
		buffer = append(buffer, generateSingleUpdateCommand())
	}

	refDocs := duplicateListRandomButSameUUID(buffer)

	updateCommandGroupsMap := make(map[string][]UpdateCommand)
	for _, uc := range buffer {
		if updateCommandGroupsMap[uc.DocumentID] != nil {
			updateCommandGroupsMap[uc.DocumentID] = append(updateCommandGroupsMap[uc.DocumentID], uc)
		} else {
			updateCommandGroupsMap[uc.DocumentID] = []UpdateCommand{uc}
		}
	}

	for _, v := range updateCommandGroupsMap {
		updateCommandGroups = append(updateCommandGroups, v)
	}

	b.ResetTimer()

	_, err := worker.applyMergesV3(updateCommandGroups, refDocs)
	if err != nil {
		b.Error(err)
	}
}

// Helper function to generate a random test_data_6
func getTestData6() string {
	rtestData6 := rand.Float64()
	if rtestData6 < 0.2 {
		return "a"
	} else if rtestData6 < 0.5 {
		return "b"
	} else if rtestData6 < 0.7 {
		return "c"
	} else {
		return ""
	}
}

// Helper function to generate a random keyword
func getRandomKeyword() string {
	keywords := []string{"KeywordA", "KeywordB", "KeywordC", "KeywordD", "KeywordE"}
	// append random generated keywords
	for i := 0; i < rand.Intn(100); i++ {
		keywords = append(keywords, randStringBytes(10))
	}

	return keywords[rand.Intn(len(keywords))]
}

func TestApplyMergesV3(t *testing.T) {
	// to test apply merges V3 we take apply merge v2 and compare results
	updateCommandGroups := make([][]UpdateCommand, 0)
	buffer := make([]UpdateCommand, 0)
	refDocs := make([]models.Document, 0)
	rand.Seed(time.Now().UnixNano())
	viper.Set("APPLY_MERGE_WORKER_COUNT", 4)

	for i := 0; i < 100; i++ {
		randUUID := uuid.New().String()

		refDocs = append(refDocs, generateRandomDoc(randUUID))
		buffer = append(buffer, UpdateCommand{
			DocumentID:   randUUID,
			DocumentType: "test",
			NewDoc:       generateRandomDoc(randUUID),
			MergeConfig:  testMergeConfig,
		})
	}

	worker := &IndexingWorkerV8{}
	updateCommandGroupsMap := make(map[string][]UpdateCommand)
	for _, uc := range buffer {
		if updateCommandGroupsMap[uc.DocumentID] != nil {
			updateCommandGroupsMap[uc.DocumentID] = append(updateCommandGroupsMap[uc.DocumentID], uc)
		} else {
			updateCommandGroupsMap[uc.DocumentID] = []UpdateCommand{uc}
		}
	}

	for _, v := range updateCommandGroupsMap {
		updateCommandGroups = append(updateCommandGroups, v)
	}

	v2, err := worker.applyMergesV2(updateCommandGroups, refDocs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	v3, err := worker.applyMergesV3(updateCommandGroups, refDocs)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	expression.AssertEqual(t, len(v2), len(v3), "apply merges v2 and v3 should have the same length")

	// normally indexes should be same
	//for _, docV2 := range v2 {
	//	found := false
	//	for _, docV3 := range v3 {
	//		if docV3.ID == docV2.ID {
	//			dv2 := docV2.Source.(map[string]interface{})
	//			dv3 := docV3.Source.(map[string]interface{})
	//			expression.AssertEqual(t, reflect.DeepEqual(dv2, dv3), true, "apply merges v2 and v3 should have the same result")
	//			found = true
	//			break
	//		}
	//	}
	//
	//	expression.AssertEqual(t, found, true, "apply merges v2 and v3 should have the same documents")
	//}
	for i := 0; i < len(v2); i++ {
		expression.AssertEqual(t, reflect.DeepEqual(v2[i].Source, v3[i].Source), true, "apply merges v2 and v3 should have the same result")
	}

	t.Logf("tested on %d documents", len(v2))
}
