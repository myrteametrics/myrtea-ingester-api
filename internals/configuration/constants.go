package config

import "github.com/myrteametrics/myrtea-sdk/v4/helpers"

// ConfigPath is the toml configuration file path
var ConfigPath = "config"

// ConfigName is the toml configuration file name
var ConfigName = "ingester-api"

// EnvPrefix is the standard environment variable prefix
var EnvPrefix = "MYRTEA"

// AllowedConfigKey list every allowed configuration key
var AllowedConfigKey = [][]helpers.ConfigKey{
	// TODO: use helpers predefined keys !
	helpers.GetGeneralConfigKeys(),
	helpers.GetHTTPServerConfigKeys(),
	helpers.GetPostgresqlConfigKeys(),
	helpers.GetElasticsearchConfigKeys(),
	{
		{Type: helpers.StringFlag, Name: "ELASTICSEARCH_HTTP_TIMEOUT", DefaultValue: "1m", Description: "Elasticsearch HTTP Client timeout"},
		{Type: helpers.StringFlag, Name: "ELASTICSEARCH_DIRECT_MULTI_GET_MODE", DefaultValue: "true", Description: "Elasticsearch direct multi-get mode enabled"},
		{Type: helpers.StringFlag, Name: "ELASTICSEARCH_MGET_BATCH_SIZE", DefaultValue: "1000", Description: "Elasticsearch Mget max batch size"},
		{Type: helpers.StringFlag, Name: "INGESTER_MAXIMUM_WORKERS", DefaultValue: "2", Description: "Typed Ingester's maximum parallel workers"},
		{Type: helpers.StringFlag, Name: "TYPEDINGESTER_QUEUE_BUFFER_SIZE", DefaultValue: "5000", Description: "Typed ingester's internal queue size"},
		{Type: helpers.StringFlag, Name: "WORKER_QUEUE_BUFFER_SIZE", DefaultValue: "5000", Description: "Worker's internal queue size"},
		{Type: helpers.StringFlag, Name: "WORKER_MAXIMUM_BUFFER_SIZE", DefaultValue: "2000", Description: "Worker's maximum buffer size"},
		{Type: helpers.StringFlag, Name: "WORKER_FORCE_FLUSH_TIMEOUT_SEC", DefaultValue: "10", Description: "Worker's force flush timeout (in seconds)"},
		{Type: helpers.StringFlag, Name: "DEBUG_DRY_RUN_ELASTICSEARCH", DefaultValue: "false", Description: "Enable ingester dry-run mode for elasticsearch (no interaction with ES)"},
	},
}
