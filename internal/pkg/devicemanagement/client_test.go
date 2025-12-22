package devicemanagement

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matryer/is"
)

func TestGetDevice(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v0/devices/device123" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`
			{
				"data":{
					"deviceID":"device123",
					"sensorID":"sensor456",
					"name":"Test Device"
				}
			}`))
		if err != nil {
			t.Fatalf("failed to write response: %v", err)
		}

	}))
	defer srv.Close()

	cfg := NewConfig(srv.URL, "http://oauth2/token", false, "client_id", "client_secret")
	cfg.UseAuth = false

	client, err := New(ctx, &cfg)

	is.NoErr(err)

	device, err := client.GetDevice(ctx, "device123")
	is.NoErr(err)
	is.Equal(device.DeviceID, "device123")
	is.Equal(device.SensorID, "sensor456")
	is.Equal(device.Name, "Test Device")

	_, err = client.GetDevice(ctx, "unknown-device")
	is.True(err == ErrNotFound)
}

func TestGetDeviceRetry(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()

	callCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount < 2 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`
			{
				"data":{
					"deviceID":"device123",
					"sensorID":"sensor456",
					"name":"Test Device"
				}
			}`))
		if err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer srv.Close()

	cfg := NewConfig(srv.URL, "http://oauth2/token", false, "client_id", "client_secret")
	cfg.UseAuth = false

	client, err := New(ctx, &cfg)

	is.NoErr(err)

	device, err := client.GetDevice(ctx, "device123")
	is.NoErr(err)
	is.Equal(device.DeviceID, "device123")
	is.Equal(device.SensorID, "sensor456")
	is.Equal(device.Name, "Test Device")

	is.Equal(callCount, 2)
}