package storage

import (
	"context"
	"fmt"
	"net/url"
	"time"

	messagecollector "github.com/diwise/iot-events/internal/pkg/messageCollector"
	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v5"
)

func (s Storage) QueryDevice(ctx context.Context, deviceID string, tenants []string) messagecollector.QueryResult {
	if deviceID == "" {
		return errorResult("query contains no deviceID")
	}

	sql := `
		SELECT "id", urn, MAX("time"), count(*) as "n"
		FROM events_measurements
		WHERE device_id = @device_id
		GROUP BY "id", urn
		ORDER BY "id";
	`

	args := pgx.NamedArgs{
		"device_id": deviceID,
		"tenants":   tenants,
	}

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		return errorResult(err.Error())
	}

	var id, urn string
	var ts time.Time
	var n uint64
	var total uint64 = 0
	lastObserved := time.Unix(0, 0)

	dr := messagecollector.DeviceResult{
		DeviceID:     deviceID,
		LastObserved: lastObserved,
		Measurements: make([]messagecollector.MeasurementType, 0),
	}

	_, err = pgx.ForEachRow(rows, []any{&id, &urn, &ts, &n}, func() error {

		u := fmt.Sprintf("/api/v0/measurements?id=%s", url.QueryEscape(id))

		t := messagecollector.MeasurementType{
			ID:           id,
			Urn:          urn,
			Count:        n,
			LastObserved: ts,
			Link:         u,
		}
		dr.Measurements = append(dr.Measurements, t)

		if ts.After(dr.LastObserved) {
			dr.LastObserved = ts
		}

		total += n
		return nil
	})
	if err != nil {
		return errorResult(err.Error())
	}

	dr.TotalCount = total

	return messagecollector.QueryResult{
		Data:       dr,
		Count:      uint64(len(dr.Measurements)),
		Offset:     0,
		Limit:      uint64(len(dr.Measurements)),
		TotalCount: uint64(len(dr.Measurements)),
		Error:      nil,
	}
}

func (s Storage) Query(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
	id, ok := q.GetString("id")
	if !ok {
		return errorResult("query contains no ID")
	}

	sql := `	
		SELECT "time",device_id,urn,"location",n,v,vs,vb,unit,tenant, count(*) OVER () AS total_count 
		FROM events_measurements
		WHERE "id" = @id 
		  AND tenant=any(@tenants)
		ORDER BY "time" ASC
		OFFSET @offset LIMIT @limit;
	`

	offset := q.GetUint64OrDefault("offset", 0)
	limit := q.GetUint64OrDefault("limit", 10)

	args := pgx.NamedArgs{
		"id":      id,
		"offset":  offset,
		"limit":   limit,
		"tenants": tenants,
	}

	var ts time.Time
	var device_id, urn, n, vs, unit, tenant string
	var location pgtype.Point
	var total_count int64
	var v *float64
	var vb *bool

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		return errorResult(err.Error())
	}

	m := messagecollector.MeasurementResult{
		ID:     id,
		Values: make([]messagecollector.Value, 0),
	}

	_, err = pgx.ForEachRow(rows, []any{&ts, &device_id, &urn, &location, &n, &v, &vs, &vb, &unit, &tenant, &total_count}, func() error {
		value := messagecollector.Value{
			Timestamp:   ts,
			BoolValue:   vb,
			StringValue: vs,
			Unit:        unit,
			Value:       v,
		}
		m.Values = append(m.Values, value)
		return nil
	})
	if err != nil {
		return errorResult(err.Error())
	}

	m.DeviceID = device_id
	m.Lat = location.P.Y
	m.Lon = location.P.X
	m.Name = n
	m.Tenant = tenant
	m.Urn = urn

	return messagecollector.QueryResult{
		Data:       m,
		Count:      uint64(len(m.Values)),
		Offset:     offset,
		Limit:      limit,
		TotalCount: uint64(total_count),
		Error:      nil,
	}
}

func errorResult(msg string) messagecollector.QueryResult {
	return messagecollector.QueryResult{
		Error: fmt.Errorf(msg),
	}
}
