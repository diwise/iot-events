package messagecollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/google/uuid"
)

type MeasurementStorer interface {
	Save(ctx context.Context, m Measurement) error
}

type MeasurementRetriever interface {
	Query(ctx context.Context, q QueryParams, tenants []string) QueryResult
	QueryDevice(ctx context.Context, deviceID string, tenants []string) QueryResult
}

type MessageCollector struct {
	m mediator.Mediator
	s MeasurementStorer
}

func NewMessageCollector(m mediator.Mediator, s MeasurementStorer) MessageCollector {
	return MessageCollector{
		m: m,
		s: s,
	}
}

func (mc *MessageCollector) Start(ctx context.Context) {
	collector := &collector{
		id:    fmt.Sprintf("message-collector-%s", uuid.NewString()),
		inbox: make(chan mediator.Message),
		done:  make(chan bool),
		s:     mc.s,
	}

	mc.m.Register(collector)

	go collector.run(ctx, mc.m)
}

type collector struct {
	id    string
	done  chan bool
	inbox chan mediator.Message
	s     MeasurementStorer
}

func (c *collector) run(ctx context.Context, m mediator.Mediator) {
	defer func() {
		m.Unregister(c)
	}()

	for {
		select {
		case <-ctx.Done():
			c.done <- true
		case <-c.done:
			return
		case msg := <-c.inbox:
			log := logging.GetFromContext(msg.Context())
			incoming := struct {
				Pack *senml.Pack `json:"pack,omitempty"`
			}{}
			err := json.Unmarshal(msg.Data(), &incoming)
			if err != nil {
				log.Error("unable to unmarshal incoiming message", "err", err.Error())
				continue
			}

			if incoming.Pack == nil {
				log.Error("message contains no pack", "body", string(msg.Data()))
				continue
			}

			err = incoming.Pack.Validate()
			if err != nil {
				log.Error("message contains an invalid pack", "err", err.Error())
				continue
			}

			ctx := logging.NewContextWithLogger(msg.Context(), log, slog.String("message_id", msg.ID()), slog.String("message_type", msg.Type()))
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			err = store(ctx, c.s, incoming.Pack.Clone())
			if err != nil {
				log.Error("could not store message", "err", err.Error())
				continue
			}
		}
	}
}

func store(ctx context.Context, s MeasurementStorer, pack senml.Pack) error {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return fmt.Errorf("could not find header record (0)")
	}

	tenant, ok := pack.GetStringValue(senml.FindByName("tenant"))
	if !ok {
		return fmt.Errorf("could not find tenant record")
	}

	deviceID := strings.Split(header.Name, "/")[0]
	urn := header.StringValue
	lat, lon, _ := pack.GetLatLon()

	var errs []error

	for _, r := range pack {
		n, err := strconv.Atoi(r.Name)
		if err != nil || n == 0 {
			continue
		}

		rec, ok := pack.GetRecord(senml.FindByName(r.Name))
		if !ok {
			log.Error("could not find record", "name", r.Name)
			continue
		}

		id := rec.Name
		name := strconv.Itoa(n)
		ts, _ := rec.GetTime()

		measurement := NewMeasurement(ts, id, deviceID, name, urn, tenant)
		measurement.BoolValue = rec.BoolValue
		measurement.Value = rec.Value
		measurement.StringValue = rec.StringValue
		measurement.Lat = lat
		measurement.Lon = lon
		measurement.Unit = rec.Unit

		err = s.Save(ctx, measurement)
		if err != nil {
			log.Error("could not store measurement", "err", err.Error())
			errs = append(errs, err)
			continue
		}
	}

	return errors.Join(errs...)
}

func (c *collector) ID() string {
	return c.id
}

func (c *collector) Tenants() []string {
	return []string{}
}

func (c *collector) Mailbox() chan mediator.Message {
	return c.inbox
}

func (c *collector) AcceptIfValid(m mediator.Message) bool {
	if !strings.Contains(m.Type(), "message.") {
		return false
	}

	c.inbox <- m

	return true
}
