package devicemanagement

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type Device struct {
	DeviceID string `json:"deviceID"`
	SensorID string `json:"sensorID"`
	Name     string `json:"name"`
}

type Config struct {
	DevMgmtUrl         string
	OAuth2TokenURL     string
	OAuthInsecureUrl   bool
	OAuth2ClientID     string
	OAuth2ClientSecret string
}

func NewConfig(devMgmtUrl, oauth2TokenURL string, oauthInsecureUrl bool, oauth2ClientID, oauth2ClientSecret string) Config {
	return Config{
		DevMgmtUrl:         devMgmtUrl,
		OAuth2TokenURL:     oauth2TokenURL,
		OAuthInsecureUrl:   oauthInsecureUrl,
		OAuth2ClientID:     oauth2ClientID,
		OAuth2ClientSecret: oauth2ClientSecret,
	}
}

type client struct {
	url               string
	httpClient        http.Client
	clientCredentials *clientcredentials.Config
	keepRunning       *atomic.Bool
	queue             chan func()
	knownDevices      map[string]*Device
}

var tracer = otel.Tracer("iot-events/devicemanagement")

//go:generate moq -rm -out devicemanagement_mock.go . Client
type Client interface {
	GetDevice(ctx context.Context, deviceID string) (*Device, error)
}

func New(ctx context.Context, cfg *Config) (Client, error) {
	oauthConfig := &clientcredentials.Config{
		ClientID:     cfg.OAuth2ClientID,
		ClientSecret: cfg.OAuth2ClientSecret,
		TokenURL:     cfg.OAuth2TokenURL,
	}

	httpTransport := http.DefaultTransport
	if cfg.OAuthInsecureUrl {
		trans, ok := httpTransport.(*http.Transport)
		if ok {
			if trans.TLSClientConfig == nil {
				trans.TLSClientConfig = &tls.Config{}
			}
			trans.TLSClientConfig.InsecureSkipVerify = true
		}
	}

	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(httpTransport),
	}

	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

	token, err := oauthConfig.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get client credentials from %s: %w", oauthConfig.TokenURL, err)
	}

	if !token.Valid() {
		return nil, fmt.Errorf("an invalid token was returned from %s", cfg.OAuth2TokenURL)
	}

	c := &client{
		url:               cfg.DevMgmtUrl,
		clientCredentials: oauthConfig,
		httpClient:        *httpClient,
		keepRunning:       &atomic.Bool{},
		queue:             make(chan func()),
		knownDevices:      make(map[string]*Device),
	}

	go c.run(ctx)

	return c, nil
}

func (c *client) run(ctx context.Context) {
	if c.keepRunning.Load() {
		return
	}

	c.keepRunning.Store(true)

	for {
		select {
		case <-ctx.Done():
			c.keepRunning.Store(false)
			return
		case fn := <-c.queue:
			fn()
		}
	}
}

func (c *client) refreshToken(ctx context.Context) (*oauth2.Token, error) {
	var err error

	ctx, span := tracer.Start(ctx, "refresh-token")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

	ctx = context.WithValue(ctx, oauth2.HTTPClient, c.httpClient)
	token, err := c.clientCredentials.Token(ctx)

	return token, err
}

var ErrNotFound error = errors.New("not found")
var errInternal error = errors.New("internal error")
var errRetry error = errors.New("retry")

func (c *client) GetDevice(ctx context.Context, deviceID string) (*Device, error) {
	if dev, ok := c.knownDevices[deviceID]; ok {
		return dev, nil
	}

	resultchan := make(chan Device)
	errchan := make(chan error)

	c.queue <- func() {
		var err error
		ctx, span := tracer.Start(ctx, "get-device")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

		deviceUrl := fmt.Sprintf("%s/api/v0/devices/%s", c.url, deviceID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, deviceUrl, nil)
		if err != nil {
			errchan <- fmt.Errorf("failed to create request: %w", err)
			return
		}

		token, err := c.refreshToken(ctx)
		if err != nil {
			errchan <- fmt.Errorf("failed to refresh token: %w", err)
			return
		}

		req.Header.Add("Authorization", "Bearer "+token.AccessToken)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			errchan <- fmt.Errorf("failed to perform request: %w", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			errchan <- ErrNotFound
			return
		}

		if resp.StatusCode != http.StatusOK {
			errchan <- fmt.Errorf("%w: received status code %d", errInternal, resp.StatusCode)
			return
		}

		response := struct {
			Data Device `json:"data"`
		}{}

		b, _ := io.ReadAll(resp.Body)
		err = json.Unmarshal(b, &response)
		if err != nil {
			errchan <- fmt.Errorf("failed to decode response body: %w", err)
			return
		}

		resultchan <- response.Data
	}

	select {
	case d := <-resultchan:
		c.knownDevices[deviceID] = &d
		return &d, nil
	case e := <-errchan:
		if errors.Is(e, errRetry) {
			time.Sleep(10 * time.Millisecond)
			return c.GetDevice(ctx, deviceID)
		}
		return nil, e
	}
}
