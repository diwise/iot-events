package devicemanagement

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matryer/is"
)

func testClient(t *testing.T, ctx context.Context, srv *httptest.Server) Client {
	t.Helper()

	cfg := NewConfig(srv.URL, "http://oauth2/token", false, "client_id", "client_secret")
	cfg.UseAuth = false

	c, err := New(ctx, &cfg)
	if err != nil {
		t.Fatalf("failed to create device client: %v", err)
	}

	return c
}

func deviceHandler(release chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if release != nil {
			<-release
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"deviceID":"device123","sensorID":"sensor456","name":"Test Device"}}`))
	}
}

// REV-007: lookups after worker shutdown must fail fast instead of
// hanging on a receiver that will never come back. Cached reads stay
// available; only uncached lookups hit the stopped worker.
func TestGetDeviceFailsFastAfterStop(t *testing.T) {
	is := is.New(t)

	srv := httptest.NewServer(deviceHandler(nil))
	defer srv.Close()

	svcCtx, cancel := context.WithCancel(t.Context())
	c := testClient(t, svcCtx, srv)
	cancel()

	impl, ok := c.(*client)
	is.True(ok)

	deadline := time.Now().Add(5 * time.Second)
	for !impl.stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("worker did not observe stop")
		}
		time.Sleep(10 * time.Millisecond)
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.GetDevice(context.Background(), "never-cached-device")
		done <- err
	}()

	select {
	case err := <-done:
		is.True(errors.Is(err, errClientStopped))
	case <-time.After(5 * time.Second):
		t.Fatal("GetDevice blocked after stop")
	}
}

// REV-007: abandoning a blocked lookup must return promptly and leave
// the worker usable for later calls.
func TestGetDeviceCallerCancelAbandonsSafely(t *testing.T) {
	is := is.New(t)

	release := make(chan struct{})
	srv := httptest.NewServer(deviceHandler(release))
	defer srv.Close()

	c := testClient(t, t.Context(), srv)

	callCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.GetDevice(callCtx, "device123")
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		is.True(errors.Is(err, context.Canceled))
	case <-time.After(5 * time.Second):
		t.Fatal("abandoned lookup did not return")
	}

	close(release)

	device, err := c.GetDevice(context.Background(), "device123")
	is.NoErr(err)
	is.Equal(device.DeviceID, "device123")
}
