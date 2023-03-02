package api

import (
	"fmt"
	"io"
	"net/http"

	"github.com/diwise/iot-events/internal/pkg/application"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"
)

func New(serviceName string, app application.App) chi.Router {
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

	r.Get("/api/events", EventSource(app))
	r.Post("/api/push", Push(app))

	return r
}

func EventSource(a application.App) http.HandlerFunc {
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

		//TODO: get tenants from token
		client := application.NewClient([]string{})

		a.Add(client)

		defer func() {
			a.Close(client)
		}()

		go func() {
			<-r.Context().Done()
			a.Close(client)
		}()

		for {
			m := <-client.Notify
			fmt.Fprintf(w, "%s", m.Format())
			flusher.Flush()
		}
	}
}

func Push(a application.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		a.Notify(application.NewMessage("", "", "default", body))
		w.WriteHeader(http.StatusOK)
	}
}
