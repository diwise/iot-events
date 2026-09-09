package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diwise/iot-events/internal/application"
	"github.com/diwise/iot-events/internal/infrastructure/cloudevents"
	"github.com/diwise/iot-events/internal/infrastructure/devicemanagement"
	"github.com/diwise/iot-events/internal/infrastructure/mqtt"
	"github.com/diwise/iot-events/internal/infrastructure/storage"
	"github.com/diwise/iot-events/internal/pkg/measurements"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/presentation/api"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

func defaultFlags() flagMap {
	return flagMap{
		listenAddress: "0.0.0.0",
		servicePort:   "8080",
		controlPort:   "8000",

		cloudeventsFile:   "/opt/diwise/config/cloudevents.yaml",
		policiesFile:      "/opt/diwise/config/authz.rego",
		authzAccessObject: "false",
		metadataFile:      "/opt/diwise/config/metadata.csv",

		messengerTopic: "#",

		dbHost:     "",
		dbUser:     "",
		dbPassword: "",
		dbPort:     "5432",
		dbName:     "diwise",
		dbSSLMode:  "disable",

		mqttEnabled:    "false",
		mqttBrokerUrl:  "tcp://localhost:1883",
		mqttUser:       "",
		mqttPassword:   "",
		mqttClientId:   "iot-events",
		mqttInsecure:   "true",
		mqttPrefix:     "devices/",
		mqttIdentifier: "deviceID",

		devMgmtUrl:         "http://iot-device-mgmt",
		oauth2TokenUrl:     "",
		oauth2ClientId:     "",
		oauth2ClientSecret: "",
		oauth2InsecureUrl:  "false",

		logLevel: "debug",
	}
}

const serviceName string = "iot-events"

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	logging.SetLogLevel(parseLogLevel(flags[logLevel]))

	ctx, cancel := context.WithCancel(ctx)

	cf, err := os.Open(flags[cloudeventsFile])
	exitIf(err, logger, "unable to open cloudevents config file")

	cloudeventsConfig, err := cloudevents.LoadConfiguration(cf)
	exitIf(err, logger, "unable to load cloudevents config")

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	var mf io.ReadCloser
	mf, err = os.Open(flags[metadataFile])
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Warn("metadata file does not exist, proceeding without initial metadata", "file", flags[metadataFile])
			mf = io.NopCloser(strings.NewReader(""))
		} else {
			exitIf(err, logger, "unable to open metadata config file")
		}
	}

	messengerConfig := messaging.LoadConfiguration(ctx, serviceName, logger)
	storageConfig := storage.NewConfig(flags[dbHost], flags[dbPort], flags[dbName], flags[dbUser], flags[dbPassword], flags[dbSSLMode])
	mqttConfig := mqtt.NewConfig(mqttEnabledFlag(flags), flags[mqttBrokerUrl], flags[mqttUser], flags[mqttPassword], []string{}, flags[mqttClientId], mqttInsecureFlag(flags), flags[mqttPrefix], flags[mqttIdentifier])
	dmcConfig := devicemanagement.NewConfig(flags[devMgmtUrl], flags[oauth2TokenUrl], oauthInsecureFlag(flags), flags[oauth2ClientId], flags[oauth2ClientSecret])

	dmcConfig.Enabled = mqttConfig.Enabled

	cfg := &appConfig{
		storageConfig:     &storageConfig,
		messengerConfig:   &messengerConfig,
		cloudeventsConfig: cloudeventsConfig,
		mqttConfig:        &mqttConfig,
		dmcConfig:         &dmcConfig,
		cancelContextFn:   cancel,
	}

	runner, err := initialize(ctx, flags, cfg, policies, mf)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile io.ReadCloser, metadataFile io.ReadCloser) (servicerunner.Runner[appConfig], error) {
	var err error

	var messenger messaging.MsgContext
	var s storage.Storage
	var ce cloudevents.CloudEvents
	var m mediator.Mediator
	var mc mqtt.Client
	var dmc devicemanagement.Client

	owned := &ownedResources{}

	// closeStorage releases an opened but not yet owned storage. It is
	// used on partial OnInit failure, where OnShutdown never runs.
	// Note: an initialized-but-never-started messenger must NOT be
	// closed here; its Close blocks until the worker loop runs.
	closeStorage := func() {
		if s != nil {
			s.Close()
			s = nil
		}
	}

	probes := readinessProbes()

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]),
			muxinit(func(ctx context.Context, identifier string, port string, svcCfg *appConfig, handler *http.ServeMux) error {
				accessObjectAuthz := accessObjectEnabled(flags)
				defer policiesFile.Close()
				return api.RegisterHandlers(ctx, serviceName, handler, m, s, policiesFile, api.WithAccessObjectAuthorization(accessObjectAuthz))
			}),
		),
		oninit(func(ctx context.Context, cfg *appConfig) error {
			defer metadataFile.Close()

			m = mediator.New(ctx)

			s, err = storage.New(ctx, *cfg.storageConfig)
			if err != nil {
				return fmt.Errorf("could not create storage %w", err)
			}

			messenger, err = messaging.Initialize(ctx, *cfg.messengerConfig)
			if err != nil {
				closeStorage()
				return fmt.Errorf("could not initialize messenger %w", err)
			}

			owned.messenger = messenger
			owned.tracker = &handlerTracker{}
			owned.cancel = cfg.cancelContextFn
			owned.storage = s

			ce = cloudevents.New(cfg.cloudeventsConfig, m)

			metadata, err := measurements.LoadMetadata(ctx, metadataFile)
			if err != nil {
				closeStorage()
				return fmt.Errorf("failed to load metadata: %w", err)
			}

			err = s.SeedMetadata(ctx, metadata)
			if err != nil {
				closeStorage()
				return fmt.Errorf("could not seed metadata %w", err)
			}

			mc, err = mqtt.NewClient(ctx, *cfg.mqttConfig)
			if err != nil {
				closeStorage()
				return fmt.Errorf("could not create mqtt client %w", err)
			}

			dmc, err = devicemanagement.New(ctx, cfg.dmcConfig)
			if err != nil {
				closeStorage()
				return fmt.Errorf("could not create device management client %w", err)
			}

			return nil
		}),
		onstarting(func(ctx context.Context, svcCfg *appConfig) (err error) {
			// OnStarting failures bypass OnShutdown in the runner, so
			// clean up acquired resources on every error path below.
			defer func() {
				if err != nil {
					owned.close(ctx)
				}
			}()

			m.Start(ctx)
			ce.Start(ctx)

			err = mqtt.Start(ctx, m, mc, svcCfg.mqttConfig.Prefix, svcCfg.mqttConfig.Identifier, dmc)
			if err != nil {
				return fmt.Errorf("could not start mqtt publisher %w", err)
			}

			messenger.Start()

			tracked := &trackingMessenger{MsgContext: messenger, tracker: owned.tracker}
			if err = tracked.RegisterTopicMessageHandler(flags[messengerTopic], application.NewMessageHandler(m)); err != nil {
				return fmt.Errorf("could not register topic handler: %w", err)
			}
			if err = tracked.RegisterTopicMessageHandler("message.accepted", measurements.NewMessageAcceptedHandler(s)); err != nil {
				return fmt.Errorf("could not register message.accepted handler: %w", err)
			}

			return nil
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			owned.close(ctx)

			return nil
		}),
	)

	return runner, nil
}

// The bool toggles below intentionally use different interpretations.
// The helpers are the minimal production seams so tests target actual
// behavior: exact "true" comparison for the MQTT/OAuth toggles,
// strconv semantics for the access-object toggle.
func mqttEnabledFlag(flags flagMap) bool {
	return flags[mqttEnabled] == "true"
}

func mqttInsecureFlag(flags flagMap) bool {
	return flags[mqttInsecure] == "true"
}

func oauthInsecureFlag(flags flagMap) bool {
	return flags[oauth2InsecureUrl] == "true"
}

func accessObjectEnabled(flags flagMap) bool {
	v, _ := strconv.ParseBool(flags[authzAccessObject])
	return v
}

// readinessProbes returns the named readiness stubs. Per harmonization
// standard they always report OK and never call any dependency. Probe
// names are preserved for external deployment definitions.
func readinessProbes() map[string]k8shandlers.ServiceProber {
	return map[string]k8shandlers.ServiceProber{
		"rabbitmq":  func(context.Context) (string, error) { return "ok", nil },
		"mqtt":      func(context.Context) (string, error) { return "ok", nil },
		"timescale": func(context.Context) (string, error) { return "ok", nil },
	}
}

// Shutdown budget for admitted handler drain, within the runner's 30s
// shutdown hook budget. The hook itself never receives the runner's
// timeout, so shutdown derives its own bound here.
const shutdownHandlerDrainTimeout = 10 * time.Second

// handlerTracker tracks admitted topic-message deliveries so shutdown
// can await them. The messaging library acknowledges on dispatch and its
// Close only joins the dispatch loop, never the handler goroutines.
type handlerTracker struct {
	wg sync.WaitGroup
}

func (t *handlerTracker) track(next messaging.TopicMessageHandler) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg messaging.IncomingTopicMessage, log *slog.Logger) {
		t.wg.Add(1)
		defer t.wg.Done()
		next(ctx, msg, log)
	}
}

// wait blocks until tracked handlers complete or the timeout elapses,
// reporting whether all handlers finished.
func (t *handlerTracker) wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.wg.Wait()
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// trackingMessenger decorates handler registration with delivery
// tracking. All other MsgContext behavior is forwarded unchanged.
type trackingMessenger struct {
	messaging.MsgContext
	tracker *handlerTracker
}

func (m *trackingMessenger) RegisterTopicMessageHandler(routingKey string, h messaging.TopicMessageHandler) error {
	return m.MsgContext.RegisterTopicMessageHandler(routingKey, m.tracker.track(h))
}

// ownedResources tracks the resources created during OnInit so shutdown
// is nil-safe, ordered and idempotent. The underlying messenger Close is
// not safe to call twice, hence the sync.Once guard.
//
// Shutdown order: stop inflow (messenger), await admitted handlers
// within budget, cancel workers, then close storage. HTTP servers stay
// live until after OnShutdown returns (runner behavior); that residual
// window is documented, not fixed here.
type ownedResources struct {
	once      sync.Once
	messenger messaging.MsgContext
	tracker   *handlerTracker
	cancel    func()
	storage   interface{ Close() }
}

func (o *ownedResources) close(context.Context) {
	o.once.Do(func() {
		if o.messenger != nil {
			o.messenger.Close()
		}
		if o.tracker != nil {
			o.tracker.wait(shutdownHandlerDrainTimeout)
		}
		if o.cancel != nil {
			o.cancel()
		}
		if o.storage != nil {
			o.storage.Close()
		}
	})
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {

	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])
	flags[controlPort] = envOrDef(ctx, "CONTROL_PORT", flags[controlPort])

	flags[messengerTopic] = envOrDef(ctx, "RABBITMQ_TOPIC", flags[messengerTopic])

	flags[dbHost] = envOrDef(ctx, "POSTGRES_HOST", flags[dbHost])
	flags[dbPort] = envOrDef(ctx, "POSTGRES_PORT", flags[dbPort])
	flags[dbName] = envOrDef(ctx, "POSTGRES_DBNAME", flags[dbName])
	flags[dbUser] = envOrDef(ctx, "POSTGRES_USER", flags[dbUser])
	flags[dbPassword] = envOrDef(ctx, "POSTGRES_PASSWORD", flags[dbPassword])
	flags[dbSSLMode] = envOrDef(ctx, "POSTGRES_SSLMODE", flags[dbSSLMode])

	flags[mqttEnabled] = envOrDef(ctx, "MQTT_ENABLED", flags[mqttEnabled])
	flags[mqttBrokerUrl] = envOrDef(ctx, "MQTT_BROKER_URL", flags[mqttBrokerUrl])
	flags[mqttUser] = envOrDef(ctx, "MQTT_USER", flags[mqttUser])
	flags[mqttPassword] = envOrDef(ctx, "MQTT_PASSWORD", flags[mqttPassword])
	flags[mqttClientId] = envOrDef(ctx, "MQTT_CLIENT_ID", flags[mqttClientId])
	flags[mqttInsecure] = envOrDef(ctx, "MQTT_INSECURE", flags[mqttInsecure])
	flags[mqttPrefix] = envOrDef(ctx, "MQTT_PREFIX", flags[mqttPrefix])
	flags[mqttIdentifier] = envOrDef(ctx, "MQTT_IDENTIFIER", flags[mqttIdentifier])

	flags[devMgmtUrl] = envOrDef(ctx, "DEV_MGMT_URL", flags[devMgmtUrl])
	flags[oauth2TokenUrl] = envOrDef(ctx, "OAUTH2_TOKEN_URL", flags[oauth2TokenUrl])
	flags[oauth2ClientId] = envOrDef(ctx, "OAUTH2_CLIENT_ID", flags[oauth2ClientId])
	flags[oauth2ClientSecret] = envOrDef(ctx, "OAUTH2_CLIENT_SECRET", flags[oauth2ClientSecret])
	flags[oauth2InsecureUrl] = envOrDef(ctx, "OAUTH2_REALM_INSECURE", flags[oauth2InsecureUrl])

	flags[authzAccessObject] = envOrDef(ctx, "AUTHZ_ACCESS_OBJECT_ENABLED", flags[authzAccessObject])

	flags[logLevel] = envOrDef(ctx, "LOG_LEVEL", flags[logLevel])

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	// Allow command line arguments to override defaults and environment variables
	flag.Func("cloudevents", "configuration file for cloud events", apply(cloudeventsFile))
	flag.Func("policies", "an authorization policy file", apply(policiesFile))
	flag.Func("authz-access-object", "enable access-object authorization policy result model", apply(authzAccessObject))
	flag.Func("metadata", "a CSV file with initial metadata", apply(metadataFile))
	flag.Func("loglevel", "set log level (debug, info, warn, error)", apply(logLevel))

	flag.Parse()

	return ctx, flags
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelDebug
	}
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		time.Sleep(1 * time.Second)
		os.Exit(1)
	}
}
