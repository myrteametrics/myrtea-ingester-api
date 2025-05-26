package app

import (
	"github.com/elastic/go-elasticsearch/v8"
	es "github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/spf13/viper"
)

func InitElasticsearch() {
	urls := viper.GetStringSlice("ELASTICSEARCH_URLS")

	es.ReplaceGlobals(elasticsearch.Config{
		Addresses:     urls,
		EnableMetrics: true,
	})
}
