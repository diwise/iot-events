package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	messagecollector "github.com/diwise/iot-events/internal/pkg/msgcollector"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

func (s storageImpl) Query(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
	_, ok := q.GetString("aggrMethods")
	if ok {
		return s.aggrQuery(ctx, q, tenants)
	}

	id, ok := q.GetString("id")
	if !ok || id == "" {
		return errorResult("query contains no ID")
	}

	log := logging.GetFromContext(ctx)

	timeRelSql, timeAt, endTimeAt, err := getTimeRelSQL(q)
	if err != nil {
		return errorResult("%s", err.Error())
	}

	/*
		sql := `
			SELECT "time",device_id,urn,"location",n,v,vs,vb,unit,tenant, count(*) OVER () AS total_count
			FROM events_measurements
		`
	*/
	sql := `	
	SELECT "time",device_id,urn,"location",n,v,vs,vb,unit,tenant 
	FROM events_measurements		
`

	where := `WHERE "id" = @id `

	if vb, ok := q.GetBool("vb"); ok {
		if vb {
			where += `AND vb IS NOT NULL AND vb=TRUE `
		} else {
			where += `AND vb IS NOT NULL AND vb=FALSE `
		}
	}

	if v, ok := q.GetString("v"); ok {
		_, err := strconv.ParseFloat(v, 64)
		if err == nil {
			v = fmt.Sprintf("=%s", v)
		}

		where += fmt.Sprintf("AND v IS NOT NULL AND v %s ", v)
	}

	where += `AND tenant=any(@tenants)`

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

	lastN, _ := q.GetBool("lastN")
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

	var ts time.Time
	var device_id, urn, n, vs, unit, tenant string
	var location pgtype.Point
	var v *float64
	var vb *bool

	c, err := s.conn.Acquire(ctx)
	if err != nil {
		log.Debug("could not acquire connection from pool", slog.String("sql", sql), slog.Any("args", args), slog.String("err", err.Error()))
		return errorResult("%s", err.Error())
	}
	defer c.Release()

	rows, err := c.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult("%s", err.Error())
	}
	defer rows.Close()

	m := messagecollector.MeasurementResult{
		ID:     id,
		Values: make([]messagecollector.Value, 0),
	}

	for rows.Next() {
		err = rows.Scan(&ts, &device_id, &urn, &location, &n, &v, &vs, &vb, &unit, &tenant)
		if err != nil {
			log.Debug("could not scan row", slog.String("sql", sql), slog.Any("args", args), slog.String("err", err.Error()))
			return errorResult("%s", err.Error())
		}

		value := messagecollector.Value{
			Timestamp:   ts.UTC(),
			BoolValue:   vb,
			StringValue: vs,
			Unit:        unit,
			Value:       v,
		}

		m.Values = append(m.Values, value)
	}

	reverse, _ := q.GetBool("reverse")
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

	var total_count uint64 = math.MaxUint64
	var count uint64 = uint64(len(m.Values))

	if count < limit {
		total_count = offset*limit - (limit - count)
	}

	return messagecollector.QueryResult{
		Data:       m,
		Count:      count,
		Offset:     offset,
		Limit:      limit,
		TotalCount: total_count,
		Error:      nil,
	}
}

func (s storageImpl) aggrQuery(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
	aggrMethods, ok := q.GetString("aggrMethods")
	if !ok {
		return errorResult("query contains no aggregate function parameter(s)")
	}

	methods := strings.Split(aggrMethods, ",")
	if len(methods) == 0 {
		return errorResult("query contains no aggregate function parameter(s)")
	}

	if slices.Contains(methods, "rate") {
		return s.rateQuery(ctx, q, tenants)
	}

	for _, m := range methods {
		if !slices.Contains([]string{"avg", "min", "max", "sum", "rate"}, m) {
			return errorResult("invalid aggrMethods, should be [avg, min, max, sum, rate] is %s", m)
		}
	}

	id, ok := q.GetString("id")
	if !ok {
		return errorResult("query contains no ID")
	}

	avgSql := "0 as average,"
	sumSql := "0 as sum,"
	minSql := "0 as min,"
	maxSql := "0 as max,"

	if slices.Contains(methods, "avg") {
		avgSql = `AVG(v) AS average,`
	}
	if slices.Contains(methods, "min") {
		minSql = `MIN(v) AS minimum,`
	}
	if slices.Contains(methods, "max") {
		maxSql = `MAX(v) AS maximum,`
	}
	if slices.Contains(methods, "sum") {
		sumSql = `SUM(v) AS total,`
	}

	sql := "SELECT " + avgSql + sumSql + minSql + maxSql
	sql = strings.TrimSuffix(sql, ",")
	sql += " FROM events_measurements WHERE \"id\" = @id AND v IS NOT NULL AND tenant=any(@tenants) "

	timeRelSql, timeAt, endTimeAt, err := getTimeRelSQL(q)
	if err != nil {
		return errorResult("%s", err.Error())
	}

	log := logging.GetFromContext(ctx)

	args := pgx.NamedArgs{
		"id":        id,
		"tenants":   tenants,
		"timeAt":    timeAt,
		"endTimeAt": endTimeAt,
	}

	sql = sql + timeRelSql

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult("%s", err.Error())
	}
	defer rows.Close()

	aggr, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[messagecollector.AggrResult])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult("%s", err.Error())
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

func (s storageImpl) rateQuery(ctx context.Context, q messagecollector.QueryParams, tenants []string) messagecollector.QueryResult {
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
		return errorResult("invalid timeUnit, should be [hour, day] is %s", timeUnit)
	}

	timeRelSql, timeAt, endTimeAt, err := getTimeRelSQL(q)
	if err != nil {
		return errorResult("%s", err.Error())
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
	if vb, ok := q.GetBool("vb"); ok {
		if vb {
			sql += `AND vb IS NOT NULL AND vb=TRUE `
		} else {
			sql += `AND vb IS NOT NULL AND vb=FALSE `
		}
	}

	if v, ok := q.GetString("v"); ok {
		_, err := strconv.ParseFloat(v, 64)
		if err == nil {
			v = fmt.Sprintf("=%s", v)
		}

		sql += fmt.Sprintf("AND v IS NOT NULL AND v %s ", v)
	}

	sql += timeRelSql + " "
	sql += "GROUP BY e ORDER BY e;"

	rows, err := s.conn.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult("%s", err.Error())
	}
	defer rows.Close()

	var ts time.Time
	var n uint64

	values := make([]messagecollector.Value, 0)

	_, err = pgx.ForEachRow(rows, []any{&ts, &n}, func() error {
		sum := float64(n)
		value := messagecollector.Value{
			Timestamp: ts.UTC(),
			Sum:       &sum,
		}
		values = append(values, value)
		return nil
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult("%s", err.Error())
	}

	result := messagecollector.MeasurementResult{
		Values: values,
	}

	return messagecollector.QueryResult{
		Data:       result,
		Count:      uint64(len(result.Values)),
		Offset:     0,
		Limit:      uint64(len(result.Values)),
		TotalCount: uint64(len(result.Values)),
		Error:      nil,
	}
}

func (s storageImpl) QueryObject(ctx context.Context, deviceID, urn string, tenants []string) messagecollector.QueryResult {
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
		return errorResult("%s", err.Error())
	}
	defer rows.Close()

	var ts time.Time
	var id, n, vs, unit, tenant string
	var location pgtype.Point
	var v *float64
	var vb *bool
	lastObserved := time.Unix(0, 0).UTC()

	or := messagecollector.MeasurementResult{
		DeviceID:     deviceID,
		Urn:          urn,
		LastObserved: &lastObserved,
		Values:       make([]messagecollector.Value, 0),
	}

	_, err = pgx.ForEachRow(rows, []any{&ts, &id, &location, &n, &v, &vs, &vb, &unit, &tenant}, func() error {
		if or.Lat == nil && location.P.Y > 0.0 {
			or.Lat = &location.P.Y
		}

		if or.Lon == nil && location.P.X > 0.0 {
			or.Lon = &location.P.X
		}

		if ts.After(*or.LastObserved) {
			utc := ts.UTC()
			or.LastObserved = &utc
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

		or.Values = append(or.Values, value)

		return nil
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return noRowsFoundError()
		}
		return errorResult("%s", err.Error())
	}

	return messagecollector.QueryResult{
		Data:       or,
		Count:      uint64(len(or.Values)),
		Offset:     0,
		Limit:      uint64(len(or.Values)),
		TotalCount: uint64(len(or.Values)),
		Error:      nil,
	}
}

func (s storageImpl) QueryDevice(ctx context.Context, deviceID string, tenants []string) messagecollector.QueryResult {
	if deviceID == "" {
		return errorResult("query contains no deviceID")
	}

	log := logging.GetFromContext(ctx)

	sql := `SELECT id, time, urn, v, vb 
			FROM events_measurements_latest 
			WHERE device_id = @device_id  
			  AND tenant=any(@tenants);`

	args := pgx.NamedArgs{
		"device_id": deviceID,
		"tenants":   tenants,
	}

	c, err := s.conn.Acquire(ctx)
	if err != nil {
		log.Debug("could not acquire connection from pool", slog.String("sql", sql), slog.Any("args", args))
		return errorResult("%s", err.Error())
	}
	defer c.Release()

	rows, err := c.Query(ctx, sql, args)
	if err != nil {
		log.Debug("query failed", slog.String("sql", sql), slog.Any("args", args))
		return errorResult("%s", err.Error())
	}
	defer rows.Close()

	var id, urn string
	var ts time.Time
	var v *float64
	var vb *bool
	lastObserved := time.Unix(0, 0).UTC()

	dr := messagecollector.MeasurementResult{
		DeviceID:     deviceID,
		LastObserved: &lastObserved,
		Values:       make([]messagecollector.Value, 0),
	}

	for rows.Next() {
		err = rows.Scan(&id, &ts, &urn, &v, &vb)
		if err != nil {
			log.Debug("could not scan row", slog.String("sql", sql), slog.Any("args", args))
			return errorResult("%s", err.Error())
		}

		u := fmt.Sprintf("/api/v0/measurements?id=%s", url.QueryEscape(id))

		_id := id
		_urn := urn
		_ts := ts.UTC()
		_v := v
		_vb := vb

		t := messagecollector.Value{
			ID:        &_id,
			Urn:       &_urn,
			Timestamp: _ts,
			Value:     _v,
			BoolValue: _vb,
			Link:      &u,
		}
		dr.Values = append(dr.Values, t)

		if ts.After(*dr.LastObserved) {
			dr.LastObserved = &_ts
		}
	}

	return messagecollector.QueryResult{
		Data:       dr,
		Count:      uint64(len(dr.Values)),
		Offset:     0,
		Limit:      uint64(len(dr.Values)),
		TotalCount: uint64(len(dr.Values)),
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
