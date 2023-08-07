package handlers

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/protobuf/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGRPC(t *testing.T) {
	t.Skip()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial("localhost:9011", opts...)
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	client := pb.NewIngesterClient(conn)

	source, err := structpb.NewStruct(map[string]interface{}{"hello": "world"})
	if err != nil {
		t.Error(err)
	}
	request := &pb.BulkIngestRequest{
		UUID:         uuid.New().String(),
		DocumentType: "document",
		MergeConfigs: []*pb.MergeConfig{
			{
				Mode:             "self",
				Type:             "mydocumenttype",
				ExistingAsMaster: true,
				Groups: []*pb.Group{
					{
						Condition: "datemillis(New.mydate1) < datemillis(Existing.mydate1)",
						FieldReplace: []string{
							"mydate1",
							"code_date",
							"lib_date",
						},
					},
					{
						Condition: "datemillis(New.mydate2) < datemillis(Existing.mydate2)",
						FieldReplace: []string{
							"mydate2",
							"code",
							"label",
						},
					},
				},
			},
		},
		Documents: []*pb.Document{
			{ID: "1", Index: "mydocumenttype_2023-01-01", IndexType: "mydocumenttype", Source: source},
		},
	}
	b, _ := protojson.Marshal(request)
	t.Log(string(b))

	response, err := client.Ingest(context.Background(), request)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	t.Log(response)
	t.Fail()
}
