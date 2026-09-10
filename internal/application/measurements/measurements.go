package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/messaging-golang/pkg/messaging"
	diwisepkg "github.com/diwise/senml/diwise"
)

type MeasurementStorer interface {
	Save(ctx context.Context, m Measurement) error
	SaveAll(ctx context.Context, m []Measurement) error
}

var (
	errMissingTenant = errors.New("senml pack contains no tenant record")
)

type MeasurementRetriever interface {
	Query(ctx context.Context, q QueryParams, tenants []string) QueryResult
	QueryDevice(ctx context.Context, deviceID string, tenants []string) QueryResult
	QueryObject(ctx context.Context, deviceID, urn string, tenants []string) QueryResult
	Fetch(ctx context.Context, deviceID string, q QueryParams, tenants []string) (map[string][]Value, error)
	FetchLatest(ctx context.Context, deviceID string, tenants []string) ([]Value, error)

	QueryWithMetadata(ctx context.Context, q QueryParams, tenants []string) QueryResult
}

func NewMessageAcceptedHandler(s MeasurementStorer) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, log *slog.Logger) error {
		var m messageAccepted

		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("could not unmarshal message accepted", "err", err.Error())
			return messaging.Permanent(err)
		}

		// Referenstid för relativa SenML-tider: mottagningstid.
		parsed, err := diwisepkg.Parse(m.Pack, time.Now().UTC())
		if err != nil {
			log.Error("invalid senml pack in message accepted", "err", err.Error())
			return messaging.Permanent(err)
		}

		deviceID := parsed.DeviceID()
		tenant := parsed.Tenant()
		if tenant == "" {
			log.Error("could not find tenant record")
			return messaging.Permanent(errMissingTenant)
		}

		ms := []Measurement{}

		// En rad per resurs och observation. ID är det fullständiga
		// recordnamnet (unikt per enhet/objekt/kanal/resurs) och URN är
		// observationens egen – aldrig headerns. Därmed kan tidsserier
		// tas ut per sensortyp och poster med samma resursnummer från
		// olika objekt skriver inte över varandra.
		for _, o := range parsed.Objects() {
			urn := o.URN()
			meta := o.Metadata()
			lat, lon := 0.0, 0.0
			if meta.Latitude != nil {
				lat = *meta.Latitude
			}
			if meta.Longitude != nil {
				lon = *meta.Longitude
			}

			for _, r := range o.Resources() {
				name := r.Name[strings.LastIndex(r.Name, "/")+1:]
				n, err := strconv.Atoi(name)
				if err != nil || n == 0 {
					continue
				}

				ts, _ := r.GetTime()

				m := NewMeasurement(ts, r.Name, deviceID, strconv.Itoa(n), urn, tenant)
				m.BoolValue = r.BoolValue
				m.Value = r.Value
				m.StringValue = r.StringValue
				m.Lat = lat
				m.Lon = lon
				m.Unit = r.Unit

				ms = append(ms, m)
			}
		}

		if len(ms) > 0 {
			if err := s.SaveAll(ctx, ms); err != nil {
				// Bevarad semantik: lagringsfel loggas och ackas.
				// Klassificering till Temporary/Permanent kräver
				// verifierad idempotens.
				log.Error("errors occurred while storing measurements", "err", err.Error())
			}
		}
		return nil
	}
}
