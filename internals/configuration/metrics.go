package config

import (
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

var (
	Hostname        = "undefined"
	MetricNamespace = "myrtea"
	MetricComponent = "ingesterapi"

	MetricPrometheusLabels = stdprometheus.Labels{"component": MetricComponent, "hostname": Hostname}
)

func InitMetricLabels(hostname string) {
	Hostname = hostname
	MetricPrometheusLabels = stdprometheus.Labels{"component": MetricComponent, "hostname": Hostname}
}
