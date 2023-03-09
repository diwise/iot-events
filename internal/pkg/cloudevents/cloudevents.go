package cloudevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"

	cloud "github.com/cloudevents/sdk-go/v2"
)

type CloudEvents interface{}
type cloudeventsImpl struct{}

func New(config *Config, m mediator.Mediator, logger zerolog.Logger) CloudEvents {
	for _, n := range config.Notifications {
		for i, s := range n.Subscribers {
			sId := fmt.Sprintf("cloud-events-%s-%d", n.ID, i)
			logger = logger.With().Str("subscriber_id", sId).Str("endpoint", s.Endpoint).Logger()
			subscriber := &ceSubscriberImpl{
				id:          sId,
				inbox:       make(chan mediator.Message),
				tenants:     []string{"default"},
				endpoint:    s.Endpoint,
				messageType: n.Type,
				logger:      logger,
			}

			m.Register(subscriber)

			go subscriber.run(m)
		}
	}

	return &cloudeventsImpl{}
}

type ceSubscriberImpl struct {
	id          string
	tenants     []string
	inbox       chan mediator.Message
	endpoint    string
	messageType string
	done        chan bool
	logger      zerolog.Logger
}

func (s *ceSubscriberImpl) ID() string {
	return s.id
}
func (s *ceSubscriberImpl) Tenants() []string {
	return s.tenants
}
func (s *ceSubscriberImpl) Mailbox() chan mediator.Message {
	return s.inbox
}
func (s *ceSubscriberImpl) AcceptIfValid(m mediator.Message) {
	if m.Type() != s.messageType {
		return
	}

	s.inbox <- m
}

func (s *ceSubscriberImpl) run(m mediator.Mediator) {
	defer func() {
		m.Unregister(s)
	}()

	for {
		select {
		case <-s.done:
			s.logger.Debug().Msg("Done!")
			return
		case msg := <-s.inbox:
			s.logger.Debug().Msgf("handle message %s:%s", msg.Type(), msg.ID())

			c, err := cloud.NewClientHTTP()
			if err != nil {
				s.logger.Error().Err(err).Msg("could not create cloud events client")
				break
			}

			var ds DeviceStatusUpdated
			err = json.Unmarshal(msg.Data(), &ds)
			if err != nil {
				s.logger.Error().Err(err).Msg("failed to unmarshal DeviceStatusUpdated")
				break
			}

			event := cloud.NewEvent()
			event.SetID(fmt.Sprintf("%s:%d", ds.DeviceID, ds.Timestamp.Unix()))
			event.SetTime(ds.Timestamp)
			event.SetSource("github.com/diwise/iot-device-mgmt")
			event.SetType("diwise.statusmessage")
			err = event.SetData(cloud.ApplicationJSON, ds)
			if err != nil {
				s.logger.Error().Err(err).Msg("failed to set data")
				break
			}

			ctx := cloud.ContextWithTarget(context.Background(), s.endpoint)
			result := c.Send(ctx, event)
			if cloud.IsUndelivered(result) || errors.Is(result, unix.ECONNREFUSED) {
				err = fmt.Errorf("%w", result)
				s.logger.Error().Err(err).Msg("faild to send message")
				s.done <- true
			}
		}
	}
}

type DeviceStatusUpdated struct {
	DeviceID     string       `json:"deviceID"`
	DeviceStatus DeviceStatus `json:"status"`
	Timestamp    time.Time    `json:"timestamp"`
}

type DeviceStatus struct {
	DeviceID     string   `json:"deviceID,omitempty"`
	BatteryLevel int      `json:"batteryLevel"`
	Code         int      `json:"statusCode"`
	Messages     []string `json:"statusMessages,omitempty"`
	Timestamp    string   `json:"timestamp"`
}
