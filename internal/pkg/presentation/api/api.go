package api

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/presentation/api/auth"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
)

func New(serviceName string, mediator mediator.Mediator, policies io.Reader, logger zerolog.Logger) chi.Router {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))

	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/api", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			authenticator, err := auth.NewAuthenticator(context.Background(), logger, policies)
			if err != nil {
				logger.Fatal().Err(err).Msg("failed to create api authenticator")
			}

			r.Use(authenticator)
			r.Get("/events", EventSource(mediator, logger))
		})

		r.Post("/push", Push(mediator))
	})

	KeepAlive(mediator)

	return r
}

func EventSource(m mediator.Mediator, logger zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)

		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
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

		for {
			m := <-subscriber.Mailbox()
			fmt.Fprint(w, formatMessage(m))
			flusher.Flush()
		}
	}
}

func KeepAlive(m mediator.Mediator) {
	msg := mediator.NewMessage("", "keep-alive", "default", nil)

	go func() {
		for {
			time.Sleep(10 * time.Second)
			m.Publish(msg)
		}
	}()
}

func formatMessage(m mediator.Message) string {
	msg := ""

	if m.ID() != "" {
		msg = msg + "id: " + m.ID() + "\n"
	}
	if m.Type() != "" {
		msg = msg + "event: " + m.Type() + "\n"
	}
	if m.Data() != nil {
		b64 := base64.StdEncoding.EncodeToString(m.Data())
		msg = msg + "data: " + b64 + "\n"
	}

	msg = msg + "\n"

	return msg
}

func Push(m mediator.Mediator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		m.Publish(mediator.NewMessage("", "", "default", body))

		w.WriteHeader(http.StatusOK)
	}
}
