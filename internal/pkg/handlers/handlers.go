package handlers

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

func NewTopicMessageHandler(messenger messaging.MsgContext, m mediator.Mediator, logger zerolog.Logger) messaging.TopicMessageHandler {
	return func(ctx context.Context, d amqp.Delivery, l zerolog.Logger) {

		msg := struct {
			Tenant *string `json:"tenant,omitempty"`
		}{}

		err := json.Unmarshal(d.Body, &msg)
		if err != nil {
			logger.Error().Err(err).Msgf("failed to unmarshal message")
			return
		}

		if msg.Tenant == nil {
			logger.Info().Msgf("message %s from %s contains no tenant information", d.MessageId, d.RoutingKey)
			return
		}

		m.Publish(mediator.NewMessage(d.MessageId, d.RoutingKey, *msg.Tenant, d.Body))
	}
}
