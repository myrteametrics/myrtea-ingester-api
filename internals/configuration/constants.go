package config

import "github.com/myrteametrics/myrtea-sdk/v4/configuration"

// ConfigPath is the toml configuration file path
var ConfigPath = "config"

// ConfigName is the toml configuration file name
var ConfigName = "ingester-api"

// EnvPrefix is the standard environment variable prefix
var EnvPrefix = "MYRTEA"

// AllowedConfigKey list every allowed configuration key
var AllowedConfigKey = []configuration.ConfigKey{
	{Type: configuration.StringFlag, Name: "DEBUG_MODE", DefaultValue: "false", Description: "Enable debug mode"},
	{Type: configuration.StringFlag, Name: "LOGGER_PRODUCTION", DefaultValue: "true", Description: "Enable or disable production log"},
	{Type: configuration.StringFlag, Name: "SERVER_PORT", DefaultValue: "9000", Description: "Server port"},
	{Type: configuration.StringFlag, Name: "SERVER_ENABLE_TLS", DefaultValue: "false", Description: "Run the server in unsecured mode (without SSL)"},
	{Type: configuration.StringFlag, Name: "SERVER_TLS_FILE_CRT", DefaultValue: "certs/server.rsa.crt", Description: "SSL certificate crt file location"},
	{Type: configuration.StringFlag, Name: "SERVER_TLS_FILE_KEY", DefaultValue: "certs/server.rsa.key", Description: "SSL certificate key file location"},
	{Type: configuration.StringFlag, Name: "API_ENABLE_CORS", DefaultValue: "false", Description: "Run the api with CORS enabled"},
	{Type: configuration.StringFlag, Name: "API_ENABLE_SECURITY", DefaultValue: "true", Description: "Run the api in unsecured mode (without authentication)"},
	{Type: configuration.StringFlag, Name: "API_ENABLE_GATEWAY_MODE", DefaultValue: "false", Description: "Run the api without external Auth API (with gateway)"},
	{Type: configuration.StringFlag, Name: "INSTANCE_NAME", DefaultValue: "myrtea", Description: "Myrtea instance name"},
	{Type: configuration.StringFlag, Name: "SWAGGER_HOST", DefaultValue: "localhost:9000", Description: "Swagger UI target hostname"},
	{Type: configuration.StringSliceFlag, Name: "ELASTICSEARCH_URLS", DefaultValue: []string{"http://localhost:9200"}, Description: "Elasticsearch URLS"},
	{Type: configuration.StringFlag, Name: "ELASTICSEARCH_HTTP_TIMEOUT", DefaultValue: "1m", Description: "Worker's force flush timeout (in seconds)"},
	{Type: configuration.StringFlag, Name: "ELASTICSEARCH_DIRECT_MULTI_GET_MODE", DefaultValue: "true", Description: "Elasticsearch direct multi-get mode enabled"},
	{Type: configuration.StringFlag, Name: "INGESTER_MAXIMUM_WORKERS", DefaultValue: "2", Description: "Typed Ingester's maximum parallel workers"},
	{Type: configuration.StringFlag, Name: "TYPEDINGESTER_QUEUE_BUFFER_SIZE", DefaultValue: "5000", Description: "Typed ingester's internal queue size"},
	{Type: configuration.StringFlag, Name: "WORKER_QUEUE_BUFFER_SIZE", DefaultValue: "5000", Description: "Worker's internal queue size"},
	{Type: configuration.StringFlag, Name: "WORKER_MAXIMUM_BUFFER_SIZE", DefaultValue: "2000", Description: "Worker's maximum buffer size"},
	{Type: configuration.StringFlag, Name: "WORKER_FORCE_FLUSH_TIMEOUT_SEC", DefaultValue: "10", Description: "Worker's force flush timeout (in seconds)"},
	{Type: configuration.StringFlag, Name: "DEBUG_DRY_RUN_ELASTICSEARCH", DefaultValue: "false", Description: "Enable ingester dry-run mode for elasticsearch (no interaction with ES)"},
}
