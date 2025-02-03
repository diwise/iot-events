package storage

import (
	"context"
	"errors"
	"log/slog"

	messagecollector "github.com/diwise/iot-events/internal/pkg/msgcollector"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
)

//go:generate moq -rm -out storage_mock.go . Storage
type Storage interface {
	messagecollector.MeasurementRetriever
	messagecollector.MeasurementStorer
}

type storageImpl struct {
	conn *pgxpool.Pool
}

func (s storageImpl) Save(ctx context.Context, m messagecollector.Measurement) error {
	return s.SaveMany(ctx, []messagecollector.Measurement{m})
}

func (s storageImpl) SaveMany(ctx context.Context, measurements []messagecollector.Measurement) error {
	log := logging.GetFromContext(ctx)

	sql := `INSERT INTO events_measurements (time,id,device_id,urn,location,n,v,vs,vb,unit,tenant,trace_id)
			VALUES (@time,@id,@device_id,@urn,point(@lon,@lat),@n,@v,@vs,@vb,@unit,@tenant,@trace_id)
			ON CONFLICT (time, id) DO UPDATE 
			SET v = COALESCE(EXCLUDED.v, events_measurements.v), 
			    vs = COALESCE(EXCLUDED.vs, events_measurements.vs), 
				vb = COALESCE(EXCLUDED.vb, events_measurements.vb),
				updated_on = CURRENT_TIMESTAMP;`

	spanCtx := trace.SpanContextFromContext(ctx)
	batch := &pgx.Batch{}

	for _, m := range measurements {
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

		if spanCtx.HasTraceID() {
			traceID := spanCtx.TraceID()
			args["trace_id"] = traceID.String()
		}

		batch.Queue(sql, args)
	}

	results := s.conn.SendBatch(ctx, batch)
	defer results.Close()

	_, err := results.Exec()
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			log.Error("could not insert new measurements", slog.String("code", pgErr.Code), slog.String("message", pgErr.Message))
		}
		return err
	}

	return nil
}

func New(ctx context.Context, config Config) (Storage, error) {
	pool, err := connect(ctx, config)
	if err != nil {
		return nil, err
	}

	err = initialize(ctx, pool)
	if err != nil {
		return nil, err
	}

	return storageImpl{
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
	ddl := `
			DO $$
			DECLARE
				n INTEGER;
			BEGIN			
				SELECT COUNT(*) INTO n
				FROM pg_class c
				JOIN pg_namespace ns ON ns.oid = c.relnamespace
				WHERE c.relname = 'events_measurements';
				
				IF n = 0 THEN
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
					updated_on  timestamp with time zone NULL, 
					trace_id 	TEXT NULL,			
					UNIQUE ("time", "id"));
					
					CREATE INDEX IF NOT EXISTS idx_measurements_filters ON events_measurements (device_id, tenant, id, time DESC) WHERE (v IS NOT NULL OR vb IS NOT NULL);
					CREATE INDEX IF NOT EXISTS idx_measurements_filters_asc ON events_measurements (device_id, tenant, id, time ASC) WHERE (v IS NOT NULL OR vb IS NOT NULL);
					CREATE INDEX IF NOT EXISTS idx_query_object ON events_measurements (device_id, urn, id, "time" DESC) INCLUDE (location, n, v, vs, vb, unit, tenant);
					CREATE INDEX IF NOT EXISTS idx_events_measurements_aggr ON events_measurements (id, tenant, v) WHERE v IS NOT NULL;
					CREATE INDEX IF NOT EXISTS idx_events_measurements_id_tenant_vb_true ON events_measurements ("id", tenant) WHERE vb IS TRUE;

					CREATE MATERIALIZED VIEW count_by_day WITH (timescaledb.continuous) AS
					SELECT time_bucket('1 day', time) AS bucket, "id", tenant, count(vb) as n
					FROM events_measurements
					WHERE vb IS TRUE AND id like '%/3200/%'
					GROUP BY bucket, "id", tenant;

				END IF;

				SELECT COUNT(*) INTO n
				FROM timescaledb_information.hypertables
				WHERE hypertable_name = 'events_measurements';

				IF n = 0 THEN				
					PERFORM create_hypertable('events_measurements', 'time');				
				END IF;
			END;
			$$;
	`

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, ddl)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	return tx.Commit(ctx)
}
