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
	"strings"
	"time"

	"github.com/diwise/iot-events/internal/pkg/application"
	"github.com/diwise/iot-events/internal/pkg/cloudevents"
	"github.com/diwise/iot-events/internal/pkg/devicemanagement"
	"github.com/diwise/iot-events/internal/pkg/measurements"
	"github.com/diwise/iot-events/internal/pkg/mediator"
	"github.com/diwise/iot-events/internal/pkg/mqtt"
	"github.com/diwise/iot-events/internal/pkg/presentation/api"
	"github.com/diwise/iot-events/internal/pkg/storage"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

func defaultFlags() flagMap {
	return flagMap{
		listenAddress: "0.0.0.0",
		servicePort:   "8080",
		controlPort:   "8000",

		cloudeventsFile: "/opt/diwise/config/cloudevents.yaml",
		policiesFile:    "/opt/diwise/config/authz.rego",
		metadataFile:    "/opt/diwise/config/metadata.csv",

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
	}
}

const serviceName string = "iot-events"

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

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
	mqttConfig := mqtt.NewConfig(flags[mqttEnabled] == "true", flags[mqttBrokerUrl], flags[mqttUser], flags[mqttPassword], []string{}, flags[mqttClientId], flags[mqttInsecure] == "true", flags[mqttPrefix], flags[mqttIdentifier])
	dmcConfig := devicemanagement.NewConfig(flags[devMgmtUrl], flags[oauth2TokenUrl], flags[oauth2InsecureUrl] == "true", flags[oauth2ClientId], flags[oauth2ClientSecret])

	cfg := &appConfig{
		storageConfig:     &storageConfig,
		messengerConfig:   &messengerConfig,
		cloudeventsConfig: cloudeventsConfig,
		mqttConfig:        &mqttConfig,
		dmcConfig:         &dmcConfig,
		cancelContextFn:   cancel,
	}

	runner, _ := initialize(ctx, flags, cfg, policies, mf)

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile io.ReadCloser, metadataFile io.ReadCloser) (servicerunner.Runner[appConfig], error) {
	defer policiesFile.Close()
	defer metadataFile.Close()

	var err error

	var messenger messaging.MsgContext
	var s storage.Storage
	var ce cloudevents.CloudEvents
	var m mediator.Mediator
	var mc mqtt.Client
	var dmc devicemanagement.Client

	probes := map[string]k8shandlers.ServiceProber{
		"rabbitmq":  func(context.Context) (string, error) { return "ok", nil },
		"timescale": func(context.Context) (string, error) { return "ok", nil },
	}

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]),
			muxinit(func(ctx context.Context, identifier string, port string, svcCfg *appConfig, handler *http.ServeMux) error {
				api.RegisterHandlers(ctx, serviceName, handler, m, s, policiesFile)
				return nil
			}),
		),
		oninit(func(ctx context.Context, cfg *appConfig) error {
			m = mediator.New(ctx)

			s, err = storage.New(ctx, *cfg.storageConfig)
			if err != nil {
				return fmt.Errorf("could not create storage %w", err)
			}

			messenger, err = messaging.Initialize(ctx, *cfg.messengerConfig)
			if err != nil {
				return fmt.Errorf("could not initialize messenger %w", err)
			}

			ce = cloudevents.New(cfg.cloudeventsConfig, m)

			metadata, err := measurements.LoadMetadata(ctx, metadataFile)
			if err != nil {
				return fmt.Errorf("failed to load metadata: %w", err)
			}

			err = s.SeedMetadata(ctx, metadata)
			if err != nil {
				return fmt.Errorf("could not seed metadata %w", err)
			}

			mc, err = mqtt.NewClient(ctx, *cfg.mqttConfig)
			if err != nil {
				return fmt.Errorf("could not create mqtt client %w", err)
			}

			dmc, err = devicemanagement.New(ctx, cfg.dmcConfig)
			if err != nil {
				return fmt.Errorf("could not create device management client %w", err)
			}

			return nil
		}),
		onstarting(func(ctx context.Context, svcCfg *appConfig) (err error) {
			m.Start(ctx)
			ce.Start(ctx)

			err = mqtt.Start(ctx, m, mc, svcCfg.mqttConfig.Prefix, svcCfg.mqttConfig.Identifier, dmc)
			if err != nil {
				return fmt.Errorf("could not start mqtt publisher %w", err)
			}

			messenger.Start()
			messenger.RegisterTopicMessageHandler(flags[messengerTopic], application.NewMessageHandler(m))
			messenger.RegisterTopicMessageHandler("message.accepted", measurements.NewMessageAcceptedHandler(s))

			return nil
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			messenger.Close()
			svcCfg.cancelContextFn()

			return nil
		}),
	)

	return runner, nil
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

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	// Allow command line arguments to override defaults and environment variables
	flag.Func("cloudevents", "configuration file for cloud events", apply(cloudeventsFile))
	flag.Func("policies", "an authorization policy file", apply(policiesFile))
	flag.Func("metadata", "a CSV file with initial metadata", apply(metadataFile))

	flag.Parse()

	return ctx, flags
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		time.Sleep(1 * time.Second)
		os.Exit(1)
	}
}
