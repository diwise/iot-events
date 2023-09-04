package handlers

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/farshidtz/senml/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

func NewTopicMessageHandler(messenger messaging.MsgContext, m mediator.Mediator, logger zerolog.Logger) messaging.TopicMessageHandler {
	return func(ctx context.Context, d amqp.Delivery, l zerolog.Logger) {

		msg := struct {
			Tenant *string     `json:"tenant,omitempty"`
			Pack   *senml.Pack `json:"pack,omitempty"`
		}{}

		err := json.Unmarshal(d.Body, &msg)
		if err != nil {
			logger.Error().Err(err).Msgf("failed to unmarshal message")
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
			return
		}

		m.Publish(mediator.NewMessage(d.MessageId, d.RoutingKey, tenant, d.Body))
	}
}
