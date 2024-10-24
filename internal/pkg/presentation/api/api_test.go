package api_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	messagecollector "github.com/diwise/iot-events/internal/pkg/msgcollector"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/go-chi/jwtauth/v5"
	"github.com/matryer/is"
)

func TestAPI(t *testing.T) {
	is := is.New(t)
	mm := &mediator.MediatorMock{
		PublishFunc: func(ctx context.Context, message mediator.Message) {},
	}
	sm := &storage.StorageMock{
		QueryFunc: func(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
			return messagecollector.QueryResult{}
		},
	}
	mux := http.NewServeMux()

	err := api.RegisterHandlers(context.Background(), "test", mux, mm, sm, io.NopCloser(strings.NewReader(policy)))
	is.NoErr(err)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, _ := testRequest(ts, http.MethodGet, "/api/v0/measurements", createJWTWithTenants([]string{"ignored"}), nil)

	is.Equal(resp.StatusCode, http.StatusOK)
	is.Equal(len(sm.QueryCalls()), 1)
	is.Equal(sm.QueryCalls()[0].Tenants, []string{"unittest"})
}

func testRequest(ts *httptest.Server, method, path string, token string, body io.Reader) (*http.Response, string) {
	req, _ := http.NewRequest(method, ts.URL+path, body)

	if len(token) > 0 {
		req.Header.Add("Authorization", "Bearer "+token)
	}

	resp, _ := http.DefaultClient.Do(req)
	respBody, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	return resp, string(respBody)
}

func createJWTWithTenants(tenants []string) string {
	tokenAuth := jwtauth.New("HS256", []byte("secret"), nil)
	_, tokenString, _ := tokenAuth.Encode(map[string]any{"user_id": 123, "azp": "diwise-frontend", "tenants": tenants})
	return tokenString
}

const policy string = `package example.authz
default allow := false
allow = response {
	response := {
		"tenants": ["unittest"]
	}
}
`
