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
	"net/http/pprof"
	"net/url"
	"regexp"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/messagecollector"
	"github.com/diwise/iot-events/internal/pkg/presentation/api/auth"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-events/api")

func New(ctx context.Context, serviceName string, mediator mediator.Mediator, storage storage.Storage, policies io.Reader) (chi.Router, error) {
	log := logging.GetFromContext(ctx)

	r := chi.NewRouter()

	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))

	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	authenticator, err := auth.NewAuthenticator(ctx, policies)
	if err != nil {
		return nil, fmt.Errorf("failed to create api authenticator: %w", err)
	}

	r.Route("/api/v0", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(authenticator)
			r.Get("/events", EventSource(mediator, log))
			r.Get("/measurements", NewQueryMeasurementsHandler(storage, log))
			r.Get("/measurements/{deviceID}", NewQueryDeviceHandler(storage, log))
		})
	})

	r.Get("/debug/pprof/allocs", pprof.Handler("allocs").ServeHTTP)
	r.Get("/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	r.Get("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)

	KeepAlive(mediator)

	return r, nil
}

func NewQueryDeviceHandler(m messagecollector.MeasurementRetriever, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer r.Body.Close()

		allowedTenants := auth.GetAllowedTenantsFromContext(r.Context())

		ctx, span := tracer.Start(r.Context(), "query-device")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger := o11y.AddTraceIDToLoggerAndStoreInContext(span, log, ctx)

		deviceID := chi.URLParam(r, "deviceID")

		if deviceID == "" {
			logger.Error("invalid url parameter", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		validateURN := func(urn string) bool {
			pattern := `^urn:oma:lwm2m:ext:\d+$`
			re := regexp.MustCompile(pattern)
			return re.MatchString(urn)
		}

		var result messagecollector.QueryResult

		urn := r.URL.Query().Get("urn")

		if urn != "" {
			if !validateURN(urn) {
				logger.Error("invalid urn", "err", err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			result = m.QueryObject(ctx, deviceID, urn, allowedTenants)
		} else {
			result = m.QueryDevice(ctx, deviceID, allowedTenants)
		}
		if result.Error != nil {
			logger.Error("could not query device", "err", result.Error.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		resp := NewApiResponse(r, result.Data, result.Count, result.TotalCount, result.Offset, result.Limit)

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(resp.Byte())
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

func KeepAlive(m mediator.Mediator) {
	ctx := context.Background()

	msg := mediator.NewMessage(ctx, "", "keep-alive", "default", nil)

	go func() {
		for {
			time.Sleep(10 * time.Second)
			m.Publish(ctx, msg)
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
