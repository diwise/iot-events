package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	messagecollector "github.com/diwise/iot-events/internal/pkg/messageCollector"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v5"
)

func (s Storage) QueryObject(ctx context.Context, deviceID, urn string, tenants []string) messagecollector.QueryResult {
	log := logging.GetFromContext(ctx)

	sql := `
		SELECT "time", id, location, n, v, vs, vb, unit, tenant
		FROM (
			SELECT "time", id, location, n, v, vs, vb, unit, tenant, ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY time DESC) as rn
			FROM events_measurements
			WHERE "device_id" = @device_id AND "urn" = @urn AND tenant=any(@tenants)
		) subquery
		WHERE rn = 1;
	`

	args := pgx.NamedArgs{
		"device_id": deviceID,
		"urn":       urn,
		"tenants":   tenants,
	}

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult(err.Error())
	}

	var ts time.Time
	var id, n, vs, unit, tenant string
	var location pgtype.Point
	var v *float64
	var vb *bool
	lastObserved := time.Unix(0, 0).UTC()

	or := messagecollector.ObjectResult{
		DeviceID:     deviceID,
		Urn:          &urn,
		LastObserved: lastObserved,
		Measurements: make([]messagecollector.Value, 0),
	}

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &location, &n, &v, &vs, &vb, &unit, &tenant}, func() error {
		if or.Lat == nil && location.P.Y > 0.0 {
			or.Lat = &location.P.Y
		}

		if or.Lon == nil && location.P.X > 0.0 {
			or.Lon = &location.P.X
		}

		if ts.After(or.LastObserved) {
			or.LastObserved = ts.UTC()
		}

		u := fmt.Sprintf("/api/v0/measurements?id=%s", url.QueryEscape(id))
		i := id
		name := n

		value := messagecollector.Value{
			Timestamp:   ts.UTC(),
			BoolValue:   vb,
			StringValue: vs,
			Unit:        unit,
			Value:       v,
			ID:          &i,
			Link:        &u,
			Name:        &name,
		}

		or.Measurements = append(or.Measurements, value)

		return nil
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult(err.Error())
	}

	return messagecollector.QueryResult{
		Data:       or,
		Count:      uint64(len(or.Measurements)),
		Offset:     0,
		Limit:      uint64(len(or.Measurements)),
		TotalCount: uint64(len(or.Measurements)),
		Error:      nil,
	}
}

func (s Storage) QueryDevice(ctx context.Context, deviceID string, tenants []string) messagecollector.QueryResult {
	if deviceID == "" {
		return errorResult("query contains no deviceID")
	}

	log := logging.GetFromContext(ctx)

	sql := `
		SELECT "id", urn, MAX("time"), count(*) as "n"
		FROM events_measurements
		WHERE device_id = @device_id AND tenant=any(@tenants)
		GROUP BY "id", urn
		ORDER BY "id";
	`

	args := pgx.NamedArgs{
		"device_id": deviceID,
		"tenants":   tenants,
	}

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult(err.Error())
	}

	var id, urn string
	var ts time.Time
	var n uint64
	var total uint64 = 0
	lastObserved := time.Unix(0, 0).UTC()

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
			LastObserved: ts.UTC(),
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
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult(err.Error())
	}

	dr.TotalCount = &total

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
	_, ok := q.GetString("aggrMethods")
	if ok {
		return s.AggrQuery(ctx, q, tenants)
	}

	id, ok := q.GetString("id")
	if !ok {
		return errorResult("query contains no ID")
	}

	timeRelSql, timeAt, endTimeAt, err := getTimeRelSQL(q)
	if err != nil {
		return errorResult(err.Error())
	}

	sql := `	
		SELECT "time",device_id,urn,"location",n,v,vs,vb,unit,tenant, count(*) OVER () AS total_count 
		FROM events_measurements		
	`

	where := `
		WHERE "id" = @id 
		  AND tenant=any(@tenants)
	`
	orderAsc := `
		ORDER BY "time" ASC		
	`
	orderDesc := `
		ORDER BY "time" DESC		
	`
	offsetLimit := `
		OFFSET @offset LIMIT @limit;
	`

	order := orderAsc

	lastN := q.GetBool("lastN")
	if lastN {
		order = orderDesc
	}

	sql = sql + where + timeRelSql + order + offsetLimit

	offset := q.GetUint64OrDefault("offset", 0)
	limit := q.GetUint64OrDefault("limit", 10)

	args := pgx.NamedArgs{
		"id":        id,
		"offset":    offset,
		"limit":     limit,
		"tenants":   tenants,
		"timeAt":    timeAt,
		"endTimeAt": endTimeAt,
	}

	log := logging.GetFromContext(ctx)

	var ts time.Time
	var device_id, urn, n, vs, unit, tenant string
	var location pgtype.Point
	var total_count int64
	var v *float64
	var vb *bool

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult(err.Error())
	}

	m := messagecollector.MeasurementResult{
		ID:     id,
		Values: make([]messagecollector.Value, 0),
	}

	_, err = pgx.ForEachRow(rows, []any{&ts, &device_id, &urn, &location, &n, &v, &vs, &vb, &unit, &tenant, &total_count}, func() error {
		value := messagecollector.Value{
			Timestamp:   ts.UTC(),
			BoolValue:   vb,
			StringValue: vs,
			Unit:        unit,
			Value:       v,
		}
		m.Values = append(m.Values, value)
		return nil
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult(err.Error())
	}

	reverse := q.GetBool("reverse")
	if reverse {
		slices.Reverse(m.Values)
	}

	m.DeviceID = device_id
	if location.P.Y > 0.0 && location.P.X > 0.0 {
		m.Lat = &location.P.Y
		m.Lon = &location.P.X
	}
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

func (s Storage) AggrQuery(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
	aggrMethods, ok := q.GetString("aggrMethods")
	if !ok {
		return errorResult("query contains no aggregate function parameter(s)")
	}

	methods := strings.Split(aggrMethods, ",")
	if len(methods) == 0 {
		return errorResult("query contains no aggregate function parameter(s)")
	}

	for _, m := range methods {
		if !slices.Contains([]string{"avg", "min", "max", "sum", "rate"}, m) {
			return errorResult(fmt.Sprintf("invalid aggrMethods, should be [avg, min, max, sum, rate] is %s", m))
		}
	}

	if slices.Contains(methods, "rate") {
		return s.RateQuery(ctx, q, tenants)
	}

	id, ok := q.GetString("id")
	if !ok {
		return errorResult("query contains no ID")
	}

	sql := `	
		SELECT
		  AVG(v) AS average,
		  SUM(v) AS total,
		  MIN(v) AS minimum,
		  MAX(v) AS maximum 
		FROM events_measurements		
	`

	where := `
		WHERE "id" = @id 
		  AND v IS NOT NULL 
		  AND tenant=any(@tenants)
	`

	timeRelSql, timeAt, endTimeAt, err := getTimeRelSQL(q)
	if err != nil {
		return errorResult(err.Error())
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"id":        id,
		"tenants":   tenants,
		"timeAt":    timeAt,
		"endTimeAt": endTimeAt,
	}

	sql = sql + where + timeRelSql

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult(err.Error())
	}

	aggr, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[messagecollector.AggrResult])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult(err.Error())
	}

	aggrResult := messagecollector.AggrResult{}
	if slices.Contains(methods, "avg") {
		aggrResult.Average = aggr.Average
	}
	if slices.Contains(methods, "min") {
		aggrResult.Minimum = aggr.Minimum
	}
	if slices.Contains(methods, "max") {
		aggrResult.Maximum = aggr.Maximum
	}
	if slices.Contains(methods, "sum") {
		aggrResult.Total = aggr.Total
	}

	return messagecollector.QueryResult{
		Data:       aggrResult,
		Count:      1,
		Offset:     0,
		Limit:      1,
		TotalCount: 1,
		Error:      nil,
	}
}

func (s Storage) RateQuery(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
	id, idOk := q.GetString("id")
	device_id, deviceIdOk := q.GetString("device_id")
	urn, urnOk := q.GetString("urn")

	aggrMethods, ok := q.GetString("aggrMethods")
	if !ok {
		return errorResult("query contains no aggregate function parameter(s)")
	}

	methods := strings.Split(aggrMethods, ",")
	if len(methods) == 0 {
		return errorResult("query contains no aggregate function parameter(s)")
	}

	if !slices.Contains(methods, "rate") {
		return errorResult("query contains no rate function")
	}

	timeUnit, ok := q.GetString("timeUnit")
	if !ok {
		return errorResult("query contains no timeUnit parameter")
	}

	if !slices.Contains([]string{"hour", "day"}, timeUnit) {
		return errorResult(fmt.Sprintf("invalid timeUnit, should be [hour, day] is %s", timeUnit))
	}

	timeRelSql, timeAt, endTimeAt, err := getTimeRelSQL(q)
	if err != nil {
		return errorResult(err.Error())
	}

	if !idOk && !deviceIdOk && !urnOk {
		duration := endTimeAt.Sub(timeAt)
		maxDuration := time.Duration(100 * 24 * time.Hour)
		if duration > maxDuration {
			return errorResult("query contains no id, deviceID or URN and duration between timeAt and endTimeAt is too large (%0.f > %0.f hours)", duration.Hours(), maxDuration.Hours())
		}
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"id":        id,
		"device_id": device_id,
		"urn":       urn,
		"tenants":   tenants,
		"timeAt":    timeAt,
		"endTimeAt": endTimeAt,
	}

	sql := fmt.Sprintf("SELECT DATE_TRUNC('%s', time) e, count(*) n FROM events_measurements ", timeUnit)
	sql += "WHERE tenant=any(@tenants) "
	if idOk {
		sql += "AND \"id\" = @id "
	}
	if deviceIdOk {
		sql += "AND \"device_id\" = @device_id "
	}
	if urnOk {
		sql += "AND \"urn\" = @urn "
	}
	sql += timeRelSql + " "
	sql += "GROUP BY e ORDER BY e;"

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult(err.Error())
	}

	var ts time.Time
	var n uint64

	result := make([]messagecollector.RateResult, 0)

	_, err = pgx.ForEachRow(rows, []any{&ts, &n}, func() error {
		value := messagecollector.RateResult{
			Timestamp: ts.UTC(),
			Count:     n,
		}
		result = append(result, value)
		return nil
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult(err.Error())
	}

	return messagecollector.QueryResult{
		Data:       result,
		Count:      uint64(len(result)),
		Offset:     0,
		Limit:      uint64(len(result)),
		TotalCount: uint64(len(result)),
		Error:      nil,
	}
}

func getTimeRelSQL(q messagecollector.QueryParams) (string, time.Time, time.Time, error) {
	timeRel, ok := q.GetString("timeRel")
	if ok {
		if !slices.Contains([]string{"after", "before", "between"}, timeRel) {
			return "", time.Time{}, time.Time{}, fmt.Errorf("invalid timeRel, should be [after, before, between] is %s", timeRel)
		}
	}

	var timeAt, endTimeAt time.Time
	var timeRelSql string = ""

	if timeRel != "" {
		timeAt, ok = q.GetTime("timeAt")
		if !ok {
			return "", time.Time{}, time.Time{}, fmt.Errorf("parameter timeAt is invalid")
		}
		if strings.EqualFold(timeRel, "between") {
			endTimeAt, ok = q.GetTime("endTimeAt")
			if !ok {
				return "", time.Time{}, time.Time{}, fmt.Errorf("parameter endTimeAt is invalid")
			}
		} else {
			endTimeAt = time.Now().UTC()
		}

		switch timeRel {
		case "after":
			timeRelSql = `
		  		AND "time" >= @timeAt			
			`
		case "before":
			timeRelSql = `
		  		AND "time" <= @timeAt			
			`
		case "between":
			timeRelSql = `
		  		AND "time" BETWEEN @timeAt AND @endTimeAt			
			`
		}
	}

	return timeRelSql, timeAt, endTimeAt, nil
}

func noRowsFoundError() messagecollector.QueryResult {
	return messagecollector.QueryResult{
		Error: messagecollector.ErrNotFound,
	}
}

func errorResult(msg string, args ...any) messagecollector.QueryResult {
	return messagecollector.QueryResult{
		Error: fmt.Errorf(msg, args...),
	}
}
