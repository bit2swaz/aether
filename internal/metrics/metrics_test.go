package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsEndpoint(t *testing.T) {
	// 1. Call metrics.Init()
	Init()

	// 2. Call metrics.IncConnection()
	IncConnection()

	// 3. Use httptest to call the /metrics handler
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler := promhttp.Handler()
	handler.ServeHTTP(w, req)

	// 4. Assert that the response body contains the string `aether_active_connections 1`
	body := w.Body.String()
	expected := "aether_active_connections 1"

	if !strings.Contains(body, expected) {
		t.Errorf("Expected response body to contain %q, but it didn't.\nBody:\n%s", expected, body)
	}

	// Verify status code
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}
}
