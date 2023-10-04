package cloudevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"golang.org/x/sys/unix"

	cloud "github.com/cloudevents/sdk-go/v2"
)

type CloudEvents interface{}
type cloudeventsImpl struct{}

type CloudEventSenderFunc = func(e eventInfo) error

var ErrCloudEventClientError = fmt.Errorf("could not create cloud event client")
var ErrMessageBadFormat = fmt.Errorf("could not set data")
var ErrConnRefused = fmt.Errorf("connection refused")

func New(config *Config, m mediator.Mediator, logger *slog.Logger) CloudEvents {
	for i, s := range config.Subscribers {

		sId := fmt.Sprintf("cloud-events-%s-%d", s.ID, i)
		logger = logger.With(
			slog.String("subscriber_id", sId),
			slog.String("endpoint", s.Endpoint),
		)

		var idPatterns []string
		for _, e := range s.Entities {
			idPatterns = append(idPatterns, e.IDPattern)
		}

		subscriber := &ceSubscriberImpl{
			id:          sId,
			inbox:       make(chan mediator.Message),
			done:        make(chan bool),
			tenants:     s.Tenants,
			endpoint:    s.Endpoint,
			messageType: s.Type,
			logger:      logger,
			idPatterns:  idPatterns,
			source:      s.Source,
			eventType:   s.EventType,
		}

		m.Register(subscriber)

		go subscriber.run(m, cloudEventSenderFunc)

	}

	return &cloudeventsImpl{}
}

type ceSubscriberImpl struct {
	done        chan bool
	endpoint    string
	id          string
	idPatterns  []string
	inbox       chan mediator.Message
	logger      *slog.Logger
	messageType string
	tenants     []string
	source      string
	eventType   string
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
func (s *ceSubscriberImpl) AcceptIfValid(m mediator.Message) bool {
	if m.Type() != s.messageType {
		return false
	}

	s.inbox <- m

	return true
}

func (s *ceSubscriberImpl) run(m mediator.Mediator, eventSenderFunc CloudEventSenderFunc) {
	defer func() {
		m.Unregister(s)
	}()

	contains := func(arr []string, str string) bool {
		for _, s := range arr {
			if strings.EqualFold(s, str) {
				return true
			}
		}
		return false
	}

	matchIfNotEmpty := func(idPatterns []string, id string) bool {
		if len(idPatterns) == 0 {
			return true // no configured id pattern will allow all id's
		}

		for _, idPattern := range s.idPatterns {
			regexpForID, err := regexp.CompilePOSIX(idPattern)
			if err != nil {
				return false
			}

			if regexpForID.MatchString(id) {
				return true
			}
		}

		return false
	}

	for {
		select {
		case b := <-s.done:
			s.logger.Debug(fmt.Sprintf("Done! %t", b))
			return
		case msg := <-s.inbox:
			s.logger.Debug("handle message", "message_type", msg.Type(), "message_id", msg.ID())

			if !contains(s.tenants, msg.Tenant()) {
				s.logger.Debug("tenant not supported by this subscriber")
				break
			}

			messageBody := messageBody{}
			err := json.Unmarshal(msg.Data(), &messageBody)
			if err != nil {
				s.logger.Error("could not unmarshal message body", "err", err.Error())
				break
			}

			var id string
			if messageBody.DeviceID != nil {
				id = *messageBody.DeviceID
			} else if messageBody.FunctionID != nil {
				id = *messageBody.FunctionID
			} else if messageBody.SensorID != nil {
				id = *messageBody.SensorID
			}

			if id == "" {
				s.logger.Error("message body missing id property")
				break
			}

			timestamp := time.Now().UTC()
			if messageBody.Timestamp != nil {
				timestamp = *messageBody.Timestamp
			}

			if !matchIfNotEmpty(s.idPatterns, id) {
				s.logger.Debug("no matching id pattern", "message_id", id)
				break
			}

			ei := eventInfo{
				id:        id,
				timestamp: timestamp,
				source:    s.source,
				eventType: s.eventType,
				endpoint:  s.endpoint,
				data:      msg.Data(),
			}

			err = eventSenderFunc(ei)

			if err != nil {
				s.logger.Error("failed to send event", "err", err.Error())
				if errors.Is(err, ErrCloudEventClientError) {
					s.done <- true
				}
				if errors.Is(err, ErrConnRefused) {
					s.done <- true
				}
			}
		}
	}
}

func cloudEventSenderFunc(evt eventInfo) error {
	c, err := cloud.NewClientHTTP()
	if err != nil {
		return ErrCloudEventClientError
	}

	id := fmt.Sprintf("%s:%d", evt.id, evt.timestamp.Unix())

	event := cloud.NewEvent()
	event.SetID(id)
	event.SetTime(evt.timestamp)
	event.SetSource(evt.source)
	event.SetType(evt.eventType)
	err = event.SetData(cloud.ApplicationJSON, evt.data)
	if err != nil {
		return ErrMessageBadFormat
	}

	ctx := cloud.ContextWithTarget(context.Background(), evt.endpoint)
	result := c.Send(ctx, event)
	if cloud.IsUndelivered(result) || errors.Is(result, unix.ECONNREFUSED) {
		return ErrConnRefused
	}

	return nil
}

type eventInfo struct {
	id        string
	timestamp time.Time
	source    string
	eventType string
	endpoint  string
	data      []byte
}

type messageBody struct {
	FunctionID *string    `json:"id,omitempty"`
	DeviceID   *string    `json:"deviceID,omitempty"`
	SensorID   *string    `json:"sensorID,omitempty"`
	Timestamp  *time.Time `json:"timestamp,omitempty"`
}
