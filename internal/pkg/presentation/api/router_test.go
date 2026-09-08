package api_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/diwise/iot-events/internal/pkg/measurements"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/matryer/is"
)

// Router migration (raw http.ServeMux sub-muxes -> service-chassis
// router): locks that the v1 route and the v0 {deviceID} route resolve
// with auth and path params intact.
func TestMigratedRoutes(t *testing.T) {
	is := is.New(t)
	mm := &mediator.MediatorMock{
		PublishFunc: func(message mediator.Message) {},
	}
	sm := &storage.StorageMock{
		QueryWithMetadataFunc: func(ctx context.Context, q measurements.QueryParams, tenants []string) measurements.QueryResult {
			return measurements.QueryResult{}
		},
		FetchFunc: func(ctx context.Context, deviceID string, q measurements.QueryParams, tenants []string) (map[string][]measurements.Value, error) {
			is.Equal(deviceID, "device-1")
			return map[string][]measurements.Value{}, nil
		},
	}
	mux := http.NewServeMux()

	err := api.RegisterHandlers(context.Background(), "test", mux, mm, sm, io.NopCloser(strings.NewReader(legacyPolicy)))
	is.NoErr(err)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, _ := testRequest(ts, http.MethodGet, "/api/v1/measurements", createJWTWithTenants([]string{"ignored"}), nil)
	is.Equal(resp.StatusCode, http.StatusOK)

	resp, _ = testRequest(ts, http.MethodGet, "/api/v0/measurements/device-1", createJWTWithTenants([]string{"ignored"}), nil)
	is.Equal(resp.StatusCode, http.StatusOK)
}
