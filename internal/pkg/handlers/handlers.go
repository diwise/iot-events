package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/farshidtz/senml/v2"
	"github.com/google/uuid"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

func NewTopicMessageHandler(messenger messaging.MsgContext, m mediator.Mediator, _ *slog.Logger) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		msg := struct {
			Tenant *string     `json:"tenant,omitempty"`
			Pack   *senml.Pack `json:"pack,omitempty"`
		}{}

		err := json.Unmarshal(itm.Body(), &msg)
		if err != nil {
			l.Error("failed to unmarshal message", "err", err.Error())
			return
		}

		tenant := ""

		if msg.Tenant != nil {
			tenant = *msg.Tenant
		} else {
			if msg.Pack != nil {
				for _, r := range *msg.Pack {
					if strings.EqualFold("tenant", r.Name) {
						tenant = r.StringValue
						break
					}
				}
			}
		}

		if tenant == "" {
			l.Debug("message contains no tenant")
			return
		}

		messageId := uuid.New().String()

		m.Publish(ctx, mediator.NewMessage(messageId, itm.TopicName(), tenant, itm.Body()))
	}
}
