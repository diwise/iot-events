package handlers

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

func NewTopicMessageHandler(messenger messaging.MsgContext, m mediator.Mediator) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) {
		messageID := uuid.New().String()

		ctx = logging.NewContextWithLogger(ctx, logger, slog.String("message_id", messageID), slog.String("topic", d.TopicName()), slog.String("message_type", d.ContentType()))

		msg := struct {
			Tenant *string     `json:"tenant,omitempty"`
			Pack   *senml.Pack `json:"pack,omitempty"`
		}{}

		err := json.Unmarshal(d.Body(), &msg)
		if err != nil {
			logger.Error("failed to unmarshal message", "err", err.Error())
			return
		}

		tenant := ""

		if msg.Pack != nil {
			t, ok := msg.Pack.GetStringValue(senml.FindByName("tenant"))
			if ok {
				tenant = t
			}
		}

		if tenant == "" && msg.Tenant != nil {
			tenant = *msg.Tenant
		}

		if tenant == "" {
			logger.Debug("message contains no tenant")
			return
		}

		m.Publish(ctx, mediator.NewMessage(ctx, messageID, d.TopicName(), tenant, d.Body()))
	}
}
