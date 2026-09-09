package application

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
)

var tracer = otel.Tracer("iot-events")

func NewMessageHandler(m mediator.Mediator) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) {
		var err error

		ctx, span := tracer.Start(ctx, "receive-message")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
		_, ctx, logger = o11y.AddTraceIDToLoggerAndStoreInContext(span, logger, ctx)

		messageID := uuid.New().String()

		topicMessage := struct {
			Tenant *string     `json:"tenant,omitempty"`
			Pack   *senml.Pack `json:"pack,omitempty"`
		}{}

		err = json.Unmarshal(d.Body(), &topicMessage)
		if err != nil {
			logger.Error("failed to unmarshal message", "err", err.Error())
			return
		}

		tenant := ""

		if topicMessage.Pack != nil {
			t, ok := topicMessage.Pack.GetStringValue(senml.FindByName("tenant"))
			if ok {
				tenant = t
			}
		}

		if tenant == "" && topicMessage.Tenant != nil {
			tenant = *topicMessage.Tenant
		}

		if tenant == "" {
			logger.Debug("message contains no tenant")
			return
		}

		ctx = logging.NewContextWithLogger(ctx, logger, slog.String("message_id", messageID), slog.String("topic", d.TopicName()), slog.String("content_type", d.ContentType()))

		msg := mediator.NewMessage(ctx, messageID, d.TopicName(), tenant, d.Body())
		m.Publish(msg)
	}
}
