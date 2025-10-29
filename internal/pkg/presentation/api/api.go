package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	messagecollector "github.com/diwise/iot-events/internal/pkg/measurements"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api/auth"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-events/api")

func RegisterHandlers(ctx context.Context, serviceName string, rootMux *http.ServeMux, mediator mediator.Mediator, storage storage.Storage, policies io.Reader) error {
	log := logging.GetFromContext(ctx)

	authenticator, err := auth.NewAuthenticator(ctx, policies)
	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	const apiPrefix0 string = "/api/v0"
	const apiPrefix1 string = "/api/v1"

	mux0 := http.NewServeMux()
	mux0.HandleFunc("GET /measurements", NewQueryMeasurementsHandler(storage, log))
	mux0.HandleFunc("GET /measurements/{deviceID}", NewFetchMeasurementsHandler(storage, log))

	v0 := http.StripPrefix(apiPrefix0, mux0)
	rootMux.Handle("GET "+apiPrefix0+"/", authenticator(v0))

	mux1 := http.NewServeMux()
	mux1.HandleFunc("GET /measurements", NewQueryMeasurementsHandler1(storage, log))

	v1 := http.StripPrefix(apiPrefix1, mux1)
	rootMux.Handle("GET "+apiPrefix1+"/", authenticator(v1))

	return nil
}

func NewQueryMeasurementsHandler1(m messagecollector.MeasurementRetriever, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "query-measurements")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		allowedTenants := auth.GetAllowedTenantsFromContext(ctx)
		if len(allowedTenants) == 0 {
			logger.Error("no allowed tenants in context")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			logger.Error("invalid query parameter", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		qp := messagecollector.ParseQuery(q)
		err = qp.ValidateParams(func(key string) bool {
			if strings.HasPrefix(key, "metadata[") && strings.HasSuffix(key, "]") {
				return true
			}

			switch key {
			case "id", "urn", "device_id", "timerel", "timeat", "endtimeat", "limit", "offset", "name", "latest":
				return true
			default:
				return false
			}
		})
		if err != nil {
			logger.Error("invalid query parameter", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result := m.Query2(ctx, qp, allowedTenants)
		if result.Error != nil {
			if !errors.Is(result.Error, messagecollector.ErrNotFound) {
				logger.Error("could not query measurements", "err", result.Error.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			result.Data = []any{}
		}

		resp := NewApiResponse(r, result.Data, result.Count, result.TotalCount, result.Offset, result.Limit)

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(resp.Byte())
	}
}

func NewFetchMeasurementsHandler(m messagecollector.MeasurementRetriever, log *slog.Logger) http.HandlerFunc {
	pattern := `^urn:oma:lwm2m.*$`
	urnPatternRegex := regexp.MustCompile(pattern)

	validateURN := func(urn string) bool {
		return urnPatternRegex.MatchString(urn)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		spanName := "fetch-measurements"

		var result any
		fetchLatest := r.URL.Query().Get("latest") == "true"

		if fetchLatest {
			spanName = "fetch-latest-measurements"
		}

		ctx, span := tracer.Start(r.Context(), spanName)
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		allowedTenants := auth.GetAllowedTenantsFromContext(ctx)
		deviceID := r.PathValue("deviceID")

		if deviceID == "" {
			logger.Error("invalid url parameter", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		urn := r.URL.Query().Get("urn")
		if urn != "" && !validateURN(urn) {
			logger.Error("invalid urn query parameter")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if fetchLatest {
			result, err = m.FetchLatest(ctx, deviceID, allowedTenants)
			if err != nil {
				logger.Error("could not fetch latest measurements for device", "device_id", deviceID, "err", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else {
			result, err = m.Fetch(ctx, deviceID, messagecollector.ParseQuery(r.URL.Query()), allowedTenants)
			if err != nil {
				logger.Error("could not fetch measurements for device", "device_id", deviceID, "err", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		response := ApiResponse{
			Data: result,
		}

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(response.Byte())
	}
}

func NewQueryMeasurementsHandler(m messagecollector.MeasurementRetriever, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		ctx, span := tracer.Start(r.Context(), "query-measurements")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		allowedTenants := auth.GetAllowedTenantsFromContext(ctx)

		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			logger.Error("invalid query parameter", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result := m.Query(ctx, messagecollector.ParseQuery(q), allowedTenants)
		if result.Error != nil {
			if !errors.Is(result.Error, messagecollector.ErrNotFound) {
				logger.Error("could not query measurements", "err", result.Error.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			result.Data = []any{}
		}

		resp := NewApiResponse(r, result.Data, result.Count, result.TotalCount, result.Offset, result.Limit)

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(resp.Byte())
	}
}
