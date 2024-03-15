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

func NewTopicMessageHandler(messenger messaging.MsgContext, m mediator.Mediator, _ *slog.Logger) messaging.TopicMessageHandler {
	return func(ctx context.Context, d messaging.IncomingTopicMessage, logger *slog.Logger) {

		ctx = logging.NewContextWithLogger(ctx, logger)

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

		if msg.Tenant != nil {
			tenant = *msg.Tenant
		}

		if tenant == "" && msg.Pack != nil {
			t, ok := msg.Pack.GetStringValue(senml.FindByName("tenant"))
			if ok {
				tenant = t
			}
		}

		if tenant == "" {
			return
		}

		m.Publish(ctx, mediator.NewMessage(uuid.New().String(), d.TopicName(), tenant, d.Body()))
	}
}
