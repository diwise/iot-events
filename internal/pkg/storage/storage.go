package storage

import (
	"context"
	"errors"
	"time"

	messagecollector "github.com/diwise/iot-events/internal/pkg/messageCollector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
)

type Storage struct {
	conn *pgxpool.Pool
}

func (s Storage) Save(ctx context.Context, m messagecollector.Measurement) error {
	sql := `INSERT INTO events_measurements (time,id,device_id,urn,location,n,v,vs,vb,unit,tenant,trace_id)
			VALUES (@time,@id,@device_id,@urn,point(@lon,@lat),@n,@v,@vs,@vb,@unit,@tenant,@trace_id)`

	args := pgx.NamedArgs{
		"time":      m.Timestamp.UTC(),
		"id":        m.ID,
		"device_id": m.DeviceID,
		"n":         m.Name,
		"urn":       m.Urn,
		"lon":       m.Lon,
		"lat":       m.Lat,
		"v":         m.Value,
		"vs":        m.StringValue,
		"vb":        m.BoolValue,
		"unit":      m.Unit,
		"tenant":    m.Tenant,
		"trace_id":  nil,
	}

	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		traceID := spanCtx.TraceID()
		args["trace_id"] = traceID.String()
	}

	_, err := s.conn.Exec(ctx, sql, args)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23505" { // duplicate key value violates unique constraint
				args["time"] = m.Timestamp.Add(1 * time.Nanosecond).UTC()
				_, err = s.conn.Exec(ctx, sql, args)
				if err != nil {
					return err
				}
			}
		}
		return err
	}

	return nil
}

func New(ctx context.Context, config Config) (Storage, error) {
	pool, err := connect(ctx, config)
	if err != nil {
		return Storage{}, err
	}

	err = initialize(ctx, pool)
	if err != nil {
		return Storage{}, err
	}

	return Storage{
		conn: pool,
	}, nil
}

func connect(ctx context.Context, config Config) (*pgxpool.Pool, error) {
	p, err := pgxpool.New(ctx, config.ConnStr())
	if err != nil {
		return nil, err
	}

	err = p.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func initialize(ctx context.Context, conn *pgxpool.Pool) error {
	createTable := `
			CREATE TABLE IF NOT EXISTS events_measurements (
			time 		TIMESTAMPTZ NOT NULL,
			id  		TEXT NOT NULL,
			device_id  	TEXT NOT NULL,
			urn		  	TEXT NOT NULL,
			location 	POINT NULL,
			n 			TEXT NOT NULL,									
			v 			NUMERIC NULL,
			vs 			TEXT NOT NULL DEFAULT '',			
			vb 			BOOLEAN NULL,			
			unit 		TEXT NOT NULL DEFAULT '',
			tenant 		TEXT NOT NULL,
			created_on  timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
			trace_id 	TEXT NULL,			
			UNIQUE ("time", "id"));
			
			ALTER TABLE events_measurements ADD COLUMN IF NOT EXISTS created_on timestamp with time zone NULL DEFAULT CURRENT_TIMESTAMP;
			ALTER TABLE events_measurements ADD COLUMN IF NOT EXISTS trace_id TEXT NULL;`

	countHyperTable := `SELECT COUNT(*) n FROM timescaledb_information.hypertables WHERE hypertable_name = 'events_measurements';`

	createHyperTable := `SELECT create_hypertable('events_measurements', 'time');`

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, createTable)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	var n int32
	err = tx.QueryRow(ctx, countHyperTable).Scan(&n)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	if n == 0 {
		_, err := tx.Exec(ctx, createHyperTable)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
	}

	return tx.Commit(ctx)
}
