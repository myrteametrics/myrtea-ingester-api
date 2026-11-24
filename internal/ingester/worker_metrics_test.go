package ingester

import (
	"errors"
	"testing"

	stdprometheus "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// counterValue reads one counter of the default registry, matching all the given label pairs.
func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := stdprometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matches(m, labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func matches(m *dto.Metric, labels map[string]string) bool {
	found := 0
	for _, l := range m.GetLabel() {
		if want, ok := labels[l.GetName()]; ok && want == l.GetValue() {
			found++
		}
	}
	return found == len(labels)
}

func TestFlushAbortedCountsByReason(t *testing.T) {
	l := map[string]string{"typedingester": "t-abort", "reason": "multiget"}
	before := counterValue(t, "myrtea_worker_flush_aborted_total", l)
	flushAborted("t-abort", "multiget")
	flushAborted("t-abort", "indices")
	if got := counterValue(t, "myrtea_worker_flush_aborted_total", l) - before; got != 1 {
		t.Fatalf("multiget aborts: got %v, want 1", got)
	}
}

func TestRecordBulkOutcome(t *testing.T) {
	ok := map[string]string{"typedingester": "t-bulk", "status": "ok"}
	bad := map[string]string{"typedingester": "t-bulk", "status": "error"}
	docs := map[string]string{"typedingester": "t-bulk"}
	okBefore := counterValue(t, "myrtea_worker_bulk_requests_total", ok)
	badBefore := counterValue(t, "myrtea_worker_bulk_requests_total", bad)
	docsBefore := counterValue(t, "myrtea_worker_bulk_failed_documents_total", docs)

	recordBulkOutcome("t-bulk", nil)
	recordBulkOutcome("t-bulk", errors.New("es down"))
	recordBulkOutcome("t-bulk", &bulkPartialError{failed: 3})

	if got := counterValue(t, "myrtea_worker_bulk_requests_total", ok) - okBefore; got != 1 {
		t.Fatalf("ok requests: got %v, want 1", got)
	}
	if got := counterValue(t, "myrtea_worker_bulk_requests_total", bad) - badBefore; got != 2 {
		t.Fatalf("error requests: got %v, want 2 (a refused request and a partial one)", got)
	}
	if got := counterValue(t, "myrtea_worker_bulk_failed_documents_total", docs) - docsBefore; got != 3 {
		t.Fatalf("failed documents: got %v, want 3 (only the partial error counts documents)", got)
	}
}
