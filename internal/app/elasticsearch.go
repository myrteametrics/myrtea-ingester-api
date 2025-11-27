package app

import (
	"crypto/tls"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
	es "github.com/myrteametrics/myrtea-sdk/v5/elasticsearch"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func InitElasticsearch() {
	urls := viper.GetStringSlice("ELASTICSEARCH_URLS")
	auth := viper.GetBool("ELASTICSEARCH_AUTH")
	insecure := viper.GetBool("ELASTICSEARCH_INSECURE")
	username := viper.GetString("ELASTICSEARCH_USERNAME")
	password := viper.GetString("ELASTICSEARCH_PASSWORD")

	// Build elasticsearch client config
	esClientConfig := elasticsearch.Config{
		Addresses: urls,
	}

	if auth {
		zap.L().Warn("ElasticSearch authentication enabled", zap.String("username", username))
		esClientConfig.Username = username
		esClientConfig.Password = password
	}

	// Apply insecure TLS if enabled
	if insecure {
		esClientConfig.Transport = &http.Transport{
			// #nosec G402 -- explicit: allow insecure TLS when configured via ELASTICSEARCH_INSECURE
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		}
		zap.L().Warn("ElasticSearch TLS verification disabled (insecure mode)")
	}

	zap.L().Info("Initializing ElasticSearch client", zap.Strings("urls", urls),
		zap.Bool("auth", auth), zap.Bool("insecure", insecure))
	err := es.ReplaceGlobals(esClientConfig)
	if err != nil {
		zap.L().Error("es.ReplaceGlobals", zap.Error(err))
	}
}
