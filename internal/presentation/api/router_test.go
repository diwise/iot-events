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
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/diwise/iot-events/internal/presentation/api"
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

// REV-004: authorization wraps the whole version subtree, including
// unknown paths — not just the registered endpoints.
func TestAuthCoversEntireVersionSubtree(t *testing.T) {
	policies := map[string]struct {
		policy string
		opts   []api.RegisterOption
	}{
		"legacy":        {legacyPolicy, nil},
		"access-object": {accessObjectPolicy, []api.RegisterOption{api.WithAccessObjectAuthorization(true)}},
	}

	for name, p := range policies {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			mm := &mediator.MediatorMock{
				PublishFunc: func(message mediator.Message) {},
			}
			sm := &storage.StorageMock{}
			mux := http.NewServeMux()

			err := api.RegisterHandlers(context.Background(), "test", mux, mm, sm, io.NopCloser(strings.NewReader(p.policy)), p.opts...)
			is.NoErr(err)

			ts := httptest.NewServer(mux)
			defer ts.Close()

			// No token anywhere under the version prefix must yield 401,
			// including paths with no registered endpoint.
			for _, tc := range []struct{ method, path string }{
				{http.MethodGet, "/api/v0/measurements"},
				{http.MethodGet, "/api/v0/not-a-route"},
				{http.MethodGet, "/api/v0/measurements/"},
				{http.MethodHead, "/api/v0/measurements"},
				{http.MethodGet, "/api/v1/not-a-route"},
			} {
				resp, _ := testRequest(ts, tc.method, tc.path, "", nil)
				is.Equal(resp.StatusCode, http.StatusUnauthorized)
			}

			// A valid token passes auth, so an unknown path yields 404.
			resp, _ := testRequest(ts, http.MethodGet, "/api/v0/not-a-route", createJWTWithTenants([]string{"ignored"}), nil)
			is.Equal(resp.StatusCode, http.StatusNotFound)
		})
	}
}
