package app

import (
	"github.com/elastic/go-elasticsearch/v8"
	es "github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func InitElasticsearch() {
	version := viper.GetInt("ELASTICSEARCH_VERSION")
	urls := viper.GetStringSlice("ELASTICSEARCH_URLS")

	switch version {
	case 7:
		fallthrough
	case 8:
		es.ReplaceGlobals(elasticsearch.Config{
			Addresses:     urls,
			EnableMetrics: true,
		})
	default:
		zap.L().Fatal("Unsupported Elasticsearch version", zap.Int("version", version))
	}
}
