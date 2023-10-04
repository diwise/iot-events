package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"
)

func New(ctx context.Context, serviceName string, mediator mediator.Mediator, policies io.Reader) (chi.Router, error) {
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
			r.Get("/events", EventSource(ctx, mediator))
		})
	})

	KeepAlive(ctx, mediator)

	return r, nil
}

func EventSource(ctx context.Context, m mediator.Mediator) http.HandlerFunc {
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

		allowedTenants := auth.GetAllowedTenantsFromContext(r.Context())
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
	msg := mediator.NewMessage("", "keep-alive", "default", nil)

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
