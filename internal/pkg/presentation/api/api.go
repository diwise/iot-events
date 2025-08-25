package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	messagecollector "github.com/diwise/iot-events/internal/pkg/msgcollector"
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

	//r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))

	authenticator, err := auth.NewAuthenticator(ctx, policies)
	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	const apiPrefix string = "/api/v0"

	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", EventSource(mediator, log))
	mux.HandleFunc("GET /measurements", NewQueryMeasurementsHandler(storage, log))
	mux.HandleFunc("GET /measurements/{deviceID}", NewFetchMeasurementsHandler(storage, log))

	routeGroup := http.StripPrefix(apiPrefix, mux)
	rootMux.Handle("GET "+apiPrefix+"/", authenticator(routeGroup))

	KeepAlive(ctx, mediator)

	return nil
}

func NewFetchMeasurementsHandler(m messagecollector.MeasurementRetriever, log *slog.Logger) http.HandlerFunc {
	pattern := `^urn:oma:lwm2m.*$`
	urnPatternRegex := regexp.MustCompile(pattern)

	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		allowedTenants := auth.GetAllowedTenantsFromContext(r.Context())

		ctx, span := tracer.Start(r.Context(), "query-device")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		deviceID := r.PathValue("deviceID")

		if deviceID == "" {
			logger.Error("invalid url parameter", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		validateURN := func(urn string) bool {
			return urnPatternRegex.MatchString(urn)
		}

		urn := r.URL.Query().Get("urn")
		if urn != "" && !validateURN(urn) {
			logger.Error("invalid urn query parameter")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var result any

		fetchLatest := r.URL.Query().Get("latest") == "true"
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

		allowedTenants := auth.GetAllowedTenantsFromContext(r.Context())

		ctx, span := tracer.Start(r.Context(), "query-measurements")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

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

func EventSource(m mediator.Mediator, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)

		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		ctx := logging.NewContextWithLogger(r.Context(), log)

		allowedTenants := auth.GetAllowedTenantsFromContext(ctx)
		subscriber := mediator.NewSubscriber(allowedTenants)

		m.Register(subscriber)

		defer func() {
			m.Unregister(subscriber)
		}()

		go func() {
			<-r.Context().Done()
			m.Unregister(subscriber)
		}()

		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		msgCache := make(map[string]time.Time)

		logger := logging.GetFromContext(ctx)

		for {
			msg := <-subscriber.Mailbox()

			cacheId := ""
			m := struct {
				DeviceID *string `json:"deviceID,omitempty"`
			}{}

			if err := json.Unmarshal(msg.Data(), &m); err == nil && m.DeviceID != nil {
				cacheId = fmt.Sprintf("%s:%s", *m.DeviceID, msg.Type())
				if t, ok := msgCache[cacheId]; ok {
					if time.Now().UTC().Before(t) {
						continue
					}
				}
			}

			_, err := w.Write(formatMessage(msg))
			if err != nil {
				logger.Error("could not write to response", "err", err.Error())
			}

			flusher.Flush()
			logger.Debug(
				"message sent to subscriber",
				"message_type", msg.Type(),
				"message_id", msg.ID(),
				"subscriber_id", subscriber.ID(),
			)

			if cacheId != "" {
				msgCache[cacheId] = time.Now().UTC().Add(30 * time.Second)
			}
		}
	}
}

func KeepAlive(ctx context.Context, m mediator.Mediator) {

	msg := mediator.NewMessage(ctx, "", "keep-alive", "default", nil)

	go func() {
		ticker := time.NewTicker(10 * time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.Publish(ctx, msg)
			}
		}
	}()
}

func formatMessage(m mediator.Message) []byte {
	var buffer bytes.Buffer

	if len(m.ID()) > 0 {
		buffer.WriteString(fmt.Sprintf("id: %s\n", m.ID()))
	}

	if m.Retry() > 0 {
		buffer.WriteString(fmt.Sprintf("retry: %d\n", m.Retry()))
	}

	if len(m.Type()) > 0 {
		buffer.WriteString(fmt.Sprintf("event: %s\n", m.Type()))
	}

	if m.Data() != nil {
		b64 := base64.StdEncoding.EncodeToString(m.Data())
		buffer.WriteString(fmt.Sprintf("data: %s\n", b64))
	}

	buffer.WriteString("\n")

	return buffer.Bytes()
}
