package cloudevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	config      *Config
	m           mediator.Mediator
	subscribers map[string]*cloudEventSubscriber
}

func New(cfg *Config, m mediator.Mediator) CloudEvents {
	return CloudEvents{
		config:      cfg,
		m:           m,
		subscribers: make(map[string]*cloudEventSubscriber),
	}
}

func (c CloudEvents) run(ctx context.Context, s *cloudEventSubscriber) {
	if _, ok := c.subscribers[s.ID()]; ok {
		return
	}

	c.m.Register(s)
	c.subscribers[s.ID()] = s

	go s.run(ctx, cloudEventSenderFunc)
}

func (c CloudEvents) Start(ctx context.Context) {
	for i, s := range c.config.Subscribers {
		sId := fmt.Sprintf("cloud-events-%s-%d", s.ID, i)

		var idPatterns []string
		for _, e := range s.Entities {
			idPatterns = append(idPatterns, e.IDPattern)
		}

		subscriber := &cloudEventSubscriber{
			id:          sId,
			inbox:       make(chan mediator.Message),
			tenants:     s.Tenants,
			endpoint:    s.Endpoint,
			messageType: s.Type,
			idPatterns:  idPatterns,
			source:      s.Source,
			eventType:   s.EventType,
		}

		c.run(ctx, subscriber)
	}
}

type cloudEventSubscriber struct {
	endpoint    string
	id          string
	idPatterns  []string
	inbox       chan mediator.Message
	messageType string
	tenants     []string
	source      string
	eventType   string
}

func (s *cloudEventSubscriber) ID() string {
	return s.id
}
func (s *cloudEventSubscriber) Tenants() []string {
	return s.tenants
}
func (s *cloudEventSubscriber) Mailbox() chan mediator.Message {
	return s.inbox
}
func (s *cloudEventSubscriber) Handle(m mediator.Message) bool {
	if m.Type() != s.messageType {
		return false
	}

	s.inbox <- m

	return true
}

func (s *cloudEventSubscriber) run(ctx context.Context, eventSenderFunc CloudEventSenderFunc) {
	contains := func(arr []string, str string) bool {
		for _, s := range arr {
			if strings.EqualFold(s, str) {
				return true
			}
		}
		return false
	}

	ifNotEmpty := func(idPatterns []string, id string) bool {
		if len(idPatterns) == 0 {
			return true // no configured id pattern will allow all id's
		}

		for _, idPattern := range idPatterns {
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

	retryQueue := newFailedEventQueue(eventSenderFunc)
	retryQueue.start(ctx)

	for {
		select {
		case <-ctx.Done():
			log := logging.GetFromContext(ctx)
			log.Info("done!", "subscriber_id", s.id, "subscriber_type", s.messageType, "event_type", s.eventType)
			return

		case msg := <-s.inbox:
			ctx := msg.Context()
			ctx = logging.NewContextWithLogger(ctx, logging.GetFromContext(ctx).With("subscriber_id", s.id, "subscriber_type", s.messageType, "event_type", s.eventType))

			log := logging.GetFromContext(ctx)

			if !contains(s.tenants, msg.Tenant()) {
				log.Debug("tenant not allowed", "tenant", msg.Tenant())
				continue
			}

			m := messageBody{}

			err := json.Unmarshal(msg.Data(), &m)
			if err != nil {
				log.Error("could not unmarshal message body", "err", err.Error())
				continue
			}

			if m.ID() == "" {
				log.Debug("message body missing id property", "message_type", msg.Type())
				continue
			}

			if !ifNotEmpty(s.idPatterns, m.ID()) {
				log.Debug("id pattern not matched", "id", m.ID())
				continue
			}

			var timestamp time.Time = time.Now()
			if m.Timestamp != nil {
				timestamp = *m.Timestamp
			}

			ei := eventInfo{
				ID:        m.ID(),
				Timestamp: timestamp.UTC(),
				Source:    s.source,
				EventType: s.eventType,
				Endpoint:  s.endpoint,
				Data:      msg.Data(),
			}

			err = eventSenderFunc(ctx, ei)
			if err != nil {
				log.Error("failed to send event, add to retry queue", "err", err.Error())
				retryQueue.enqueue(ei)
			}
		}
	}
}

func cloudEventSenderFunc(ctx context.Context, evt eventInfo) error {
	log := logging.GetFromContext(ctx)

	c, err := otelo11y.NewClientHTTP([]cehttp.Option{}, []client.Option{})
	if err != nil {
		return ErrCloudEventClientError
	}

	id := fmt.Sprintf("%s:%d", evt.ID, evt.Timestamp.Unix())

	event := cloudevents.NewEvent()
	event.SetID(id)
	event.SetTime(evt.Timestamp)
	event.SetSource(evt.Source)
	event.SetType(evt.EventType)
	err = event.SetData(cloudevents.ApplicationJSON, evt.Data)
	if err != nil {
		return ErrMessageBadFormat
	}

	ctx = cloudevents.ContextWithTarget(ctx, evt.Endpoint)

	result := c.Send(ctx, event)
	if cloudevents.IsUndelivered(result) || errors.Is(result, unix.ECONNREFUSED) {
		return ErrConnRefused
	}

	log.Debug(fmt.Sprintf("send cloudevent %s to %s", evt.EventType, evt.Endpoint))

	return nil
}

type retryEvent struct {
	info    eventInfo
	attempt int
}

type failedEventQueue struct {
	sender CloudEventSenderFunc
	queue  chan retryEvent
	delay  time.Duration
}

func newFailedEventQueue(sender CloudEventSenderFunc) *failedEventQueue {
	return &failedEventQueue{
		sender: sender,
		queue:  make(chan retryEvent, 1024),
		delay:  250 * time.Millisecond,
	}
}

func (q *failedEventQueue) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-q.queue:
				if err := q.sender(ctx, evt.info); err != nil {
					evt.attempt++
					delay := q.backoffDelay(evt.attempt)
					time.AfterFunc(delay, func() {
						select {
						case <-ctx.Done():
							return
						case q.queue <- evt:
						}
					})
				}
			}
		}
	}()
}

func (q *failedEventQueue) enqueue(info eventInfo) {
	q.queue <- retryEvent{info: info}
}

func (q *failedEventQueue) backoffDelay(attempt int) time.Duration {
	shift := minInt(attempt-1, 6)
	return q.delay << shift
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type eventInfo struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"`
	EventType string    `json:"type"`
	Endpoint  string    `json:"endpoint"`
	Data      []byte    `json:"data"`
}

type messageBody struct {
	FunctionID *string     `json:"id,omitempty"`
	DeviceID   *string     `json:"deviceID,omitempty"`
	SensorID   *string     `json:"sensorID,omitempty"`
	Timestamp  *time.Time  `json:"timestamp,omitempty"`
	Pack       *senml.Pack `json:"pack,omitempty"`
}

func (m messageBody) ID() string {
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
