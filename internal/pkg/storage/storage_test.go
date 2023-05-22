package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestSQL(t *testing.T) {
	// start TimescaleDB using 'docker compose -f deployments/docker-compose.yaml up'
	// test will PASS if no DB is running

	s, ctx, err := testSetup()
	if err != nil {
		return
	}

	j, _ := json.Marshal(TestObject{DeviceID: "deviceID", MessageType: "messageType"})

	err = s.Add(ctx, "deviceID", "messageType", j, time.Now())
	if err != nil {
		t.Error(err)
	}
}

func testSetup() (Storage, context.Context, error) {
	cfg := Config{
		host:     "localhost",
		user:     "diwise",
		password: "diwise",
		port:     "5432",
		dbname:   "diwise",
		sslmode:  "disable",
	}

	ctx := context.Background()

	s, err := Connect(ctx, zerolog.Logger{}, cfg)
	if err != nil {
		return nil, nil, err
	}

	_ = s.Initialize(ctx)

	return s, ctx, nil
}

type TestObject struct {
	DeviceID string `json:"deviceID"`
	MessageType string `json:"messageType"`
}