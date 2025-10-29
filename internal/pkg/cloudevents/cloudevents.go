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
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"golang.org/x/sys/unix"

	otelo11y "github.com/cloudevents/sdk-go/observability/opentelemetry/v2/client"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
)

type CloudEventSenderFunc = func(ctx context.Context, e eventInfo) error

var ErrCloudEventClientError = fmt.Errorf("could not create cloud event client")
var ErrMessageBadFormat = fmt.Errorf("could not set data")
var ErrConnRefused = fmt.Errorf("connection refused")

type CloudEvents struct {
	config *Config
	m      mediator.Mediator
}

func New(cfg *Config, m mediator.Mediator) CloudEvents {
	return CloudEvents{
		config: cfg,
		m:      m,
	}
}

func (c CloudEvents) Start(ctx context.Context) {
	ch := make(chan string)
	subscribers := map[string]*ceSubscriberImpl{}

	for i, s := range c.config.Subscribers {
		sId := fmt.Sprintf("cloud-events-%s-%d", s.ID, i)

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
			idPatterns:  idPatterns,
			source:      s.Source,
			eventType:   s.EventType,
			kill:        ch,
		}

		c.m.Register(subscriber)
		subscribers[subscriber.id] = subscriber

		go subscriber.run(ctx, cloudEventSenderFunc)
	}

	go func() {
		for s := range ch {
			if sub, ok := subscribers[s]; ok {
				c.m.Unregister(sub)
			}
		}
	}()
}

type ceSubscriberImpl struct {
	done        chan bool
	endpoint    string
	id          string
	idPatterns  []string
	inbox       chan mediator.Message
	messageType string
	tenants     []string
	source      string
	eventType   string
	retry       int
	kill        chan string
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
func (s *ceSubscriberImpl) Handle(m mediator.Message) bool {
	if m.Type() != s.messageType {
		return false
	}

	s.inbox <- m

	return true
}

func (s *ceSubscriberImpl) run(ctx context.Context, eventSenderFunc CloudEventSenderFunc) {
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

	log := logging.GetFromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			s.done <- true

		case b := <-s.done:
			log.Debug(fmt.Sprintf("Done! %t", b))
			s.kill <- s.id
			return

		case msg := <-s.inbox:
			if !contains(s.tenants, msg.Tenant()) {
				break
			}

			logger := logging.GetFromContext(msg.Context())
			messageBody := messageBody{}

			err := json.Unmarshal(msg.Data(), &messageBody)
			if err != nil {
				logger.Error("could not unmarshal message body", "err", err.Error())
				break
			}

			id := getIDFromMessage(messageBody)

			if id == "" {
				logger.Debug("message body missing id property", slog.String("message_type", msg.Type()))
				break
			}

			if !matchIfNotEmpty(s.idPatterns, id) {
				break
			}

			timestamp := time.Now().UTC()
			if messageBody.Timestamp != nil {
				timestamp = *messageBody.Timestamp
			}

			ei := eventInfo{
				id:        id,
				timestamp: timestamp,
				source:    s.source,
				eventType: s.eventType,
				endpoint:  s.endpoint,
				data:      msg.Data(),
			}

			err = eventSenderFunc(ctx, ei)
			if err != nil {
				logger.Error("failed to send event", "err", err.Error())
				if errors.Is(err, ErrCloudEventClientError) {
					s.retry++
				}
				if errors.Is(err, ErrConnRefused) {
					s.retry++
				}
			} else {
				s.retry = 0
			}

			if s.retry >= 10 {
				logger.Error(fmt.Sprintf("%s has failed to send event %d times, giving up", s.id, s.retry))
				s.done <- true
			}
		}
	}
}

func getIDFromMessage(m messageBody) string {
	if m.Pack != nil {
		rec, ok := m.Pack.GetRecord(senml.FindByName("0"))
		if ok {
			parts := strings.Split(rec.Name, "/")
			if len(parts) > 0 && len(parts[0]) > 0 {
				return parts[0]
			}
		}
	}

	if m.DeviceID != nil {
		return *m.DeviceID
	}

	if m.FunctionID != nil {
		return *m.FunctionID
	}

	if m.SensorID != nil {
		return *m.SensorID
	}

	return ""
}

func cloudEventSenderFunc(ctx context.Context, evt eventInfo) error {
	//c, err := cloudevents.NewClientHTTP()
	c, err := otelo11y.NewClientHTTP([]cehttp.Option{}, []client.Option{})
	if err != nil {
		return ErrCloudEventClientError
	}

	id := fmt.Sprintf("%s:%d", evt.id, evt.timestamp.Unix())

	event := cloudevents.NewEvent()
	event.SetID(id)
	event.SetTime(evt.timestamp)
	event.SetSource(evt.source)
	event.SetType(evt.eventType)
	err = event.SetData(cloudevents.ApplicationJSON, evt.data)
	if err != nil {
		return ErrMessageBadFormat
	}

	ctx = cloudevents.ContextWithTarget(ctx, evt.endpoint)

	result := c.Send(ctx, event)
	if cloudevents.IsUndelivered(result) || errors.Is(result, unix.ECONNREFUSED) {
		return ErrConnRefused
	}

	log := logging.GetFromContext(ctx)
	log.Debug(fmt.Sprintf("send cloudevent %s to %s", evt.eventType, evt.endpoint))

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
	FunctionID *string     `json:"id,omitempty"`
	DeviceID   *string     `json:"deviceID,omitempty"`
	SensorID   *string     `json:"sensorID,omitempty"`
	Timestamp  *time.Time  `json:"timestamp,omitempty"`
	Pack       *senml.Pack `json:"pack,omitempty"`
}
