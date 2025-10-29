package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
)

type MeasurementStorer interface {
	Save(ctx context.Context, m Measurement) error
	SaveAll(ctx context.Context, m []Measurement) error
}

type MeasurementRetriever interface {
	Query(ctx context.Context, q QueryParams, tenants []string) QueryResult
	QueryDevice(ctx context.Context, deviceID string, tenants []string) QueryResult
	QueryObject(ctx context.Context, deviceID, urn string, tenants []string) QueryResult
	Fetch(ctx context.Context, deviceID string, q QueryParams, tenants []string) (map[string][]Value, error)
	FetchLatest(ctx context.Context, deviceID string, tenants []string) ([]Value, error)

	Query2(ctx context.Context, q QueryParams, tenants []string) QueryResult
}

func NewMessageAcceptedHandler(s MeasurementStorer) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, log *slog.Logger) {
		var m messageAccepted

		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("could not unmarshal message accepted", "err", err.Error())
			return
		}

		if len(m.Pack) == 0 {
			log.Debug("senml pack contains no records")
			return
		}

		err = m.Pack.Validate()
		if err != nil {
			log.Error("invalid senml pack in message accepted", "err", err.Error())
			return
		}

		pack := m.Pack.Clone()

		header, ok := pack.GetRecord(senml.FindByName("0"))
		if !ok {
			log.Error("could not find header record (0)")
			return
		}

		tenant, ok := pack.GetStringValue(senml.FindByName("tenant"))
		if !ok {
			log.Error("could not find tenant record")
			return
		}

		deviceID := strings.Split(header.Name, "/")[0]
		urn := header.StringValue
		lat, lon, _ := pack.GetLatLon()

		errs := []error{}
		ms := []Measurement{}

		for _, r := range pack {
			n, err := strconv.Atoi(r.Name)
			if err != nil || n == 0 {
				continue
			}

			rec, ok := pack.GetRecord(senml.FindByName(r.Name))
			if !ok {
				log.Warn("could not find record", "name", r.Name)
				continue
			}

			id := rec.Name
			name := strconv.Itoa(n)
			ts, _ := rec.GetTime()

			m := NewMeasurement(ts, id, deviceID, name, urn, tenant)
			m.BoolValue = rec.BoolValue
			m.Value = rec.Value
			m.StringValue = rec.StringValue
			m.Lat = lat
			m.Lon = lon
			m.Unit = rec.Unit

			ms = append(ms, m)
		}

		if len(ms) > 0 {
			err := s.SaveAll(ctx, ms)
			if err != nil {
				errs = append(errs, err)
			}
		}

		if len(errs) > 0 {
			err := errors.Join(errs...)
			log.Error("errors occurred while storing measurements", "err", err.Error())
			return
		}

		log.Debug("measurements stored successfully")
	}
}
