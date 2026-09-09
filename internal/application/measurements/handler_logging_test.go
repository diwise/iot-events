package measurements

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// captureHandler is a minimal slog.Handler that collects log records
// for assertions. Groups are flattened; preformatted attributes are
// attached to every captured record.
type captureHandler struct {
	records *[]slog.Record
	attrs   []slog.Attr
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	r.AddAttrs(h.attrs...)
	*h.records = append(*h.records, r)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &captureHandler{records: h.records, attrs: merged}
}

func (h *captureHandler) WithGroup(string) slog.Handler {
	return &captureHandler{records: h.records, attrs: h.attrs}
}

func recordText(r slog.Record) string {
	var sb strings.Builder
	sb.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		sb.WriteString(" ")
		sb.WriteString(a.Value.String())
		return true
	})
	return sb.String()
}

// EVENTS-005: locks that handler error logs never carry the message
// body. Full sensor payloads must not end up in logs.
func TestMessageAcceptedHandlerNeverLogsBody(t *testing.T) {
	is := is.New(t)

	body := `not-json{{{secret-marker-abc123`
	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/json" },
	}

	var records []slog.Record
	log := slog.New(&captureHandler{records: &records})

	NewMessageAcceptedHandler(nil)(context.Background(), msg, log)

	is.Equal(len(records), 1)
	is.Equal(records[0].Message, "could not unmarshal message accepted")
	is.Equal(strings.Contains(recordText(records[0]), body), false)
}
