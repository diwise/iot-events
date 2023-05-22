package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

type storageSubscriber struct {
	done    chan bool
	id      string
	inbox   chan mediator.Message
	logger  zerolog.Logger
	tenants []string
	db      Storage
}

func New(m mediator.Mediator, log zerolog.Logger) mediator.Subscriber {
	ctx := context.Background()

	store, err := Connect(ctx, log, LoadConfiguration(log))
	if err != nil {
		return nil
	}

	err = store.Initialize(ctx)
	if err != nil {
		return nil
	}

	s := &storageSubscriber{
		id:      "events-storage",
		inbox:   make(chan mediator.Message),
		done:    make(chan bool),
		logger:  log,
		tenants: []string{},
		db:      store,
	}

	m.Register(s)
	go s.run(m)

	return s
}

func (s *storageSubscriber) ID() string {
	return s.id
}

func (s *storageSubscriber) AcceptIfValid(m mediator.Message) bool {
	return true
}

func (s *storageSubscriber) Tenants() []string {
	return s.tenants
}

func (s *storageSubscriber) Mailbox() chan mediator.Message {
	return s.inbox
}

func (s *storageSubscriber) run(m mediator.Mediator) {
	defer func() {
		m.Unregister(s)
	}()

	for {
		select {
		case <-s.done:
			return
		case msg := <-s.inbox:
			data := struct {
				DeviceID *string `json:"deviceID,omitempty"`
			}{}

			err := json.Unmarshal(msg.Data(), &data)
			if err != nil {
				s.logger.Error().Err(err).Msg("failed to unmarshal data")
				break
			}

			if data.DeviceID == nil {
				s.logger.Debug().Msg("no deviceID found in message")
				break
			}

			err = s.db.Add(context.Background(), *data.DeviceID, msg.Type(), msg.Data(), msg.Timestamp())
			if err != nil {
				s.logger.Error().Err(err).Msg("failed to store data")
			}
		}
	}
}

//go:generate moq -rm -out storage_mock.go . Storage
type Storage interface {
	Initialize(context.Context) error
	Add(ctx context.Context, deviceID, messageType string, data []byte, timestamp time.Time) error
}

type impl struct {
	db *pgxpool.Pool
}

type Config struct {
	host     string
	user     string
	password string
	port     string
	dbname   string
	sslmode  string
}

func (c Config) ConnStr() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", c.user, c.password, c.host, c.port, c.dbname, c.sslmode)
}

func LoadConfiguration(log zerolog.Logger) Config {
	return Config{
		host:     env.GetVariableOrDefault(log, "POSTGRES_HOST", ""),
		user:     env.GetVariableOrDefault(log, "POSTGRES_USER", ""),
		password: env.GetVariableOrDefault(log, "POSTGRES_PASSWORD", ""),
		port:     env.GetVariableOrDefault(log, "POSTGRES_PORT", "5432"),
		dbname:   env.GetVariableOrDefault(log, "POSTGRES_DBNAME", "diwise"),
		sslmode:  env.GetVariableOrDefault(log, "POSTGRES_SSLMODE", "disable"),
	}
}

func Connect(ctx context.Context, log zerolog.Logger, cfg Config) (Storage, error) {
	conn, err := pgxpool.New(ctx, cfg.ConnStr())
	if err != nil {
		return nil, err
	}

	err = conn.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return &impl{
		db: conn,
	}, nil
}

func (i *impl) Initialize(ctx context.Context) error {
	return i.createTables(ctx)
}

func (i *impl) createTables(ctx context.Context) error {
	ddl := `
		CREATE TABLE IF NOT EXISTS devices (
			id TEXT PRIMARY KEY NOT NULL
	  	);

		CREATE TABLE IF NOT EXISTS devices_events (
			time         TIMESTAMPTZ NOT NULL,
			device_id    TEXT NOT NULL,
			message_type TEXT NOT NULL,
			data         jsonb,
			FOREIGN KEY (device_id) REFERENCES devices (id)
	  	);

		CREATE INDEX IF NOT EXISTS devices_events_idx ON devices_events (device_id, message_type);`

	tx, err := i.db.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, ddl)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	var n int32
	err = tx.QueryRow(ctx, `
		SELECT COUNT(*) n
		FROM timescaledb_information.hypertables
		WHERE hypertable_name = 'devices_events';`).Scan(&n)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	if n == 0 {
		_, err := tx.Exec(ctx, `SELECT create_hypertable('devices_events', 'time');`)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (i *impl) Add(ctx context.Context, deviceID, messageType string, data []byte, timestamp time.Time) error {
	_, err := i.db.Exec(ctx, `INSERT INTO devices (id) VALUES ($1) ON CONFLICT (id) DO NOTHING;`, deviceID)
	if err != nil {
		return err
	}

	_, err = i.db.Exec(ctx, `
		INSERT INTO devices_events (time, device_id, message_type, data) VALUES ($1, $2, $3, $4);
	`, timestamp, deviceID, messageType, data)

	return err
}
