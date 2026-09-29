package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

type storerMock struct {
	saveAll func(ctx context.Context, m []Measurement) error
}

func (m *storerMock) Save(ctx context.Context, v Measurement) error {
	return m.saveAll(ctx, []Measurement{v})
}

func (m *storerMock) SaveAll(ctx context.Context, v []Measurement) error {
	return m.saveAll(ctx, v)
}

func acceptedBody(t *testing.T, tenant string) []byte {
	t.Helper()

	v := 21.5
	pack := senml.Pack{
		{
			BaseName:    "device-1/3303/",
			BaseTime:    float64(time.Now().Unix()),
			Name:        "0",
			StringValue: "urn:oma:lwm2m:ext:3303",
		},
		{
			Name:  "5700",
			Value: &v,
		},
		{
			Name:        "tenant",
			StringValue: tenant,
		},
	}

	body, err := json.Marshal(messageAccepted{Pack: pack, Timestamp: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}

	return body
}

func TestSaveAllErrorIsTemporary(t *testing.T) {
	is := is.New(t)

	storer := &storerMock{
		saveAll: func(ctx context.Context, m []Measurement) error {
			return errors.New("db down")
		},
	}

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return acceptedBody(t, "default") },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/json" },
	}

	err := NewMessageAcceptedHandler(storer)(context.Background(), msg, slog.Default())
	is.True(err != nil)
	is.True(!messaging.IsPermanent(err))
}

func TestMissingTenantIsPermanent(t *testing.T) {
	is := is.New(t)

	called := false
	storer := &storerMock{
		saveAll: func(ctx context.Context, m []Measurement) error {
			called = true
			return nil
		},
	}

	pack := senml.Pack{
		{
			BaseName:    "device-1/3303/",
			BaseTime:    float64(time.Now().Unix()),
			Name:        "0",
			StringValue: "urn:oma:lwm2m:ext:3303",
		},
	}
	body, _ := json.Marshal(messageAccepted{Pack: pack, Timestamp: time.Now().UTC()})

	msg := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return body },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/json" },
	}

	err := NewMessageAcceptedHandler(storer)(context.Background(), msg, slog.Default())
	is.True(messaging.IsPermanent(err))
	is.True(!called)
}
