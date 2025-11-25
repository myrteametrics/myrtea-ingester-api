package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v5/connector"

	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/app"
	config "github.com/myrteametrics/myrtea-ingester-api/v5/internals/configuration"
	"github.com/myrteametrics/myrtea-ingester-api/v5/internals/routes"
	"github.com/myrteametrics/myrtea-sdk/v5/helpers"
	"github.com/myrteametrics/myrtea-sdk/v5/router"
	"github.com/myrteametrics/myrtea-sdk/v5/server"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	// Version is the binary version (tag) + build number (CI pipeline)
	Version string
	// BuildDate is the date of build
	BuildDate string
)

// @title Myrtea Ingester API Swagger
// @version 1.0
// @description Myrtea Ingester API Swagger
// @termsOfService http://swagger.io/terms/

// @contact.name Myrtea Metrics
// @contact.url https://www.myrteametrics.com/fr/
// @contact.email contact@myrteametrics.com

// @host localhost:9001
// @BasePath /api/v1
func main() {
	hostname, _ := os.Hostname()
	config.InitMetricLabels(hostname)

	helpers.InitializeConfig(config.AllowedConfigKey, config.ConfigName, config.ConfigPath, config.EnvPrefix)
	zapConfig := helpers.InitLogger(viper.GetBool("LOGGER_PRODUCTION"))

	zap.L().Info("Starting Ingester-API...", zap.String("version", Version), zap.String("build_date", BuildDate))

	zap.L().Info("Initialize Elasticsearch client...")
	app.InitElasticsearch()
	zap.L().Info("Initialize Elasticsearch client... Done")

	serverPort := viper.GetInt("HTTP_SERVER_PORT")
	serverEnableTLS := viper.GetBool("HTTP_SERVER_ENABLE_TLS")
	serverTLSCert := viper.GetString("HTTP_SERVER_TLS_FILE_CRT")
	serverTLSKey := viper.GetString("HTTP_SERVER_TLS_FILE_KEY")

	done := make(chan os.Signal, 1)
	apiKey := viper.GetString("ENGINE_API_KEY")

	router := router.NewChiRouterSimple(router.ConfigSimple{
		Production:              viper.GetBool("LOGGER_PRODUCTION"),
		CORS:                    viper.GetBool("HTTP_SERVER_API_ENABLE_CORS"),
		Security:                viper.GetBool("HTTP_SERVER_API_ENABLE_SECURITY"),
		GatewayMode:             viper.GetBool("HTTP_SERVER_API_ENABLE_GATEWAY_MODE"),
		Restarter:               connector.NewRestarter(done, apiKey),
		VerboseError:            false,
		AuthenticationMode:      "BASIC",
		LogLevel:                zapConfig.Level,
		MetricsNamespace:        "myrtea",
		MetricsPrometheusLabels: nil,
		MetricsServiceName:      "",
		PublicRoutes:            make(map[string]http.Handler),
		ProtectedRoutes: map[string]http.Handler{
			"/ingester": routes.IngesterRoutes(),
		},
	})
	var srv *http.Server
	if serverEnableTLS {
		srv = server.NewSecuredServer(serverPort, serverTLSCert, serverTLSKey, router)
	} else {
		srv = server.NewUnsecuredServer(serverPort, router)
	}

	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		var err error
		if serverEnableTLS {
			err = srv.ListenAndServeTLS(serverTLSCert, serverTLSKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			zap.L().Fatal("Server listen", zap.Error(err))
		}
	}()
	zap.L().Info("Server Started", zap.String("addr", srv.Addr))

	<-done

	ctxShutDown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
	}()

	if err := srv.Shutdown(ctxShutDown); err != nil {
		zap.L().Fatal("Server shutdown failed", zap.Error(err))
	}
	zap.L().Info("Server shutdown")
}
