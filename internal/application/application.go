package application

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/diwise/iot-events/internal/infrastructure/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
)

var tracer = otel.Tracer("iot-events")

func NewMessageHandler(m mediator.Mediator) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) error {
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
			return messaging.Permanent(err)
		}

		tenant := ""

		if topicMessage.Pack != nil {
			// Tenant via diwise-API:t så att även kvalificerad
			// packmetadata (<device>/tenant) hittas. Faller tillbaka
			// på envelopens tenantfält för främmande meddelandetyper.
			if parsed, err := diwisepkg.Parse(*topicMessage.Pack, time.Now().UTC()); err == nil {
				tenant = parsed.Tenant()
			} else if t, ok := topicMessage.Pack.GetStringValue(senml.FindByName("tenant")); ok {
				tenant = t
			}
		}

		if tenant == "" && topicMessage.Tenant != nil {
			tenant = *topicMessage.Tenant
		}

		if tenant == "" {
			logger.Debug("message contains no tenant")
			return nil
		}

		ctx = logging.NewContextWithLogger(ctx, logger, slog.String("message_id", messageID), slog.String("topic", d.TopicName()), slog.String("content_type", d.ContentType()))

		msg := mediator.NewMessage(ctx, messageID, d.TopicName(), tenant, d.Body())
		m.Publish(msg)
		return nil
	}
}
