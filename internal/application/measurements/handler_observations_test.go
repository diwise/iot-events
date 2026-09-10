package measurements

import (
	"context"
	"log/slog"
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

type recordingStorer struct {
	saved [][]Measurement
	err   error
}

func (s *recordingStorer) Save(ctx context.Context, m Measurement) error { return s.err }
func (s *recordingStorer) SaveAll(ctx context.Context, m []Measurement) error {
	s.saved = append(s.saved, m)
	return s.err
}

func acceptedMessage(body string) *messaging.IncomingTopicMessageMock {
	return &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		TopicNameFunc:   func() string { return "message.accepted" },
		ContentTypeFunc: func() string { return "application/json" },
	}
}

const legacyTempAccepted = `{
	"pack":[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	],
	"timestamp":"2024-07-03T09:46:40Z"
}`

const multiAccepted = `{
	"pack":[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/3304/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3304"},
		{"n":"5700","u":"%RH","v":55.0},
		{"bn":"dev1/3301/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3301"},
		{"n":"5700","u":"lux","v":320.0},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	],
	"timestamp":"2024-07-03T09:46:40Z"
}`

// Fas C: legacy-enobjekt lagras med fullständigt ID och observationens URN.
func TestHandlerStoresLegacySingleTemp(t *testing.T) {
	is := is.New(t)
	store := &recordingStorer{}
	log := slog.Default()

	is.NoErr(NewMessageAcceptedHandler(store)(context.Background(), acceptedMessage(legacyTempAccepted), log))
	is.Equal(len(store.saved), 1)
	is.Equal(len(store.saved[0]), 1)

	m := store.saved[0][0]
	is.Equal(m.ID, "dev1/3303/5700")
	is.Equal(m.DeviceID, "dev1")
	is.Equal(m.Name, "5700")
	is.Equal(m.Urn, "urn:oma:lwm2m:ext:3303")
	is.Equal(m.Tenant, "acme")
	is.True(m.Value != nil && *m.Value == 21.5)
	is.Equal(m.Unit, "Cel")
	is.Equal(m.Timestamp.Unix(), int64(1720000000))
}

// Fas C: tre observationer ger tre rader med varsin URN – ingen skriver
// över någon annan trots samma resursnummer och tidpunkt.
func TestHandlerStoresEachObservationWithOwnURN(t *testing.T) {
	is := is.New(t)
	store := &recordingStorer{}
	log := slog.Default()

	is.NoErr(NewMessageAcceptedHandler(store)(context.Background(), acceptedMessage(multiAccepted), log))
	is.Equal(len(store.saved), 1)
	is.Equal(len(store.saved[0]), 3)

	byID := map[string]Measurement{}
	for _, m := range store.saved[0] {
		byID[m.ID] = m
	}
	is.Equal(len(byID), 3)

	temp := byID["dev1/3303/5700"]
	is.Equal(temp.Urn, "urn:oma:lwm2m:ext:3303")
	is.True(temp.Value != nil && *temp.Value == 21.5)

	hum := byID["dev1/3304/5700"]
	is.Equal(hum.Urn, "urn:oma:lwm2m:ext:3304")
	is.True(hum.Value != nil && *hum.Value == 55.0)

	lux := byID["dev1/3301/5700"]
	is.Equal(lux.Urn, "urn:oma:lwm2m:ext:3301")
	is.True(lux.Value != nil && *lux.Value == 320.0)
}

// Fas C: strukturskada är permanent – retry kan aldrig läka den.
func TestHandlerRejectsMalformedPermanently(t *testing.T) {
	is := is.New(t)
	store := &recordingStorer{}
	log := slog.Default()

	for _, body := range []string{
		`{not-json`,
		`{"pack":[],"timestamp":"2024-07-03T09:46:40Z"}`,
		`{"pack":[{"bn":"dev1/3303/","n":"5700","v":1}],"timestamp":"2024-07-03T09:46:40Z"}`,
	} {
		err := NewMessageAcceptedHandler(store)(context.Background(), acceptedMessage(body), log)
		is.True(err != nil)
		is.True(messaging.IsPermanent(err))
	}
	is.Equal(len(store.saved), 0)
}

// Fas C: pack utan tenant avvisas – tenant ska komma från berikning i core.
func TestHandlerRejectsMissingTenant(t *testing.T) {
	is := is.New(t)
	store := &recordingStorer{}
	log := slog.Default()

	body := `{"pack":[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5}
	],"timestamp":"2024-07-03T09:46:40Z"}`

	err := NewMessageAcceptedHandler(store)(context.Background(), acceptedMessage(body), log)
	is.True(err != nil)
	is.True(messaging.IsPermanent(err))
	is.Equal(len(store.saved), 0)
}
