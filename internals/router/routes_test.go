package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/handlers"
)

func TestHandlerIsAlive(t *testing.T) {
	req, err := http.NewRequest("GET", "/isalive", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handlers.IsAlive)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	expected := `{"alive": true}`
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v", rr.Body.String(), expected)
	}
}
