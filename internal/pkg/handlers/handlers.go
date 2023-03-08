package handlers

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

func NewTopicMessageHandler(messenger messaging.MsgContext, m mediator.Mediator) messaging.TopicMessageHandler {
	return func(ctx context.Context, d amqp.Delivery, l zerolog.Logger) {

		//TODO: filter messages by routingKey

		//TODO: get tenant from header
		tenant := "default"

		m.Publish(mediator.NewMessage(d.MessageId, d.RoutingKey, tenant, d.Body))
	}
}
