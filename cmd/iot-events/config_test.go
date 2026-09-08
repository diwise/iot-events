package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"testing"

	"github.com/matryer/is"
)

func withCleanFlags(t *testing.T, args []string) {
	t.Helper()

	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	})

	flag.CommandLine = flag.NewFlagSet(args[0], flag.ContinueOnError)
	os.Args = args
}

// HARM-004: locks all current defaults.
func TestDefaultFlags(t *testing.T) {
	is := is.New(t)

	flags := defaultFlags()

	expected := map[flagType]string{
		listenAddress:      "0.0.0.0",
		servicePort:        "8080",
		controlPort:        "8000",
		cloudeventsFile:    "/opt/diwise/config/cloudevents.yaml",
		policiesFile:       "/opt/diwise/config/authz.rego",
		authzAccessObject:  "false",
		metadataFile:       "/opt/diwise/config/metadata.csv",
		messengerTopic:     "#",
		dbHost:             "",
		dbUser:             "",
		dbPassword:         "",
		dbPort:             "5432",
		dbName:             "diwise",
		dbSSLMode:          "disable",
		mqttEnabled:        "false",
		mqttBrokerUrl:      "tcp://localhost:1883",
		mqttUser:           "",
		mqttPassword:       "",
		mqttClientId:       "iot-events",
		mqttInsecure:       "true",
		mqttPrefix:         "devices/",
		mqttIdentifier:     "deviceID",
		devMgmtUrl:         "http://iot-device-mgmt",
		oauth2TokenUrl:     "",
		oauth2ClientId:     "",
		oauth2ClientSecret: "",
		oauth2InsecureUrl:  "false",
		logLevel:           "debug",
	}

	is.Equal(len(flags), len(expected))
	for key, want := range expected {
		is.Equal(flags[key], want)
	}
}

// HARM-004: locks env override precedence over defaults.
func TestEnvOverrides(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-events"})

	t.Setenv("SERVICE_PORT", "9090")
	t.Setenv("CONTROL_PORT", "9001")
	t.Setenv("RABBITMQ_TOPIC", "message.accepted")
	t.Setenv("MQTT_ENABLED", "true")
	t.Setenv("MQTT_PREFIX", "sensors/")
	t.Setenv("DEV_MGMT_URL", "http://dm:8080")
	t.Setenv("LOG_LEVEL", "info")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[servicePort], "9090")
	is.Equal(flags[controlPort], "9001")
	is.Equal(flags[messengerTopic], "message.accepted")
	is.Equal(flags[mqttEnabled], "true")
	is.Equal(flags[mqttPrefix], "sensors/")
	is.Equal(flags[devMgmtUrl], "http://dm:8080")
	is.Equal(flags[logLevel], "info")
}

// HARM-004: locks CLI-over-env precedence.
func TestCLIOverridesEnv(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-events", "-loglevel=error", "-metadata=/tmp/m.csv"})

	t.Setenv("LOG_LEVEL", "info")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[logLevel], "error")
	is.Equal(flags[metadataFile], "/tmp/m.csv")
}

// HARM-004: locks the current limitation that LISTEN_ADDRESS exists in
// the flag model but can be controlled neither via env nor CLI.
// Changing this is a deliberate decision with external impact.
func TestListenAddressNotExternallyConfigurable(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-events"})

	t.Setenv("LISTEN_ADDRESS", "127.0.0.1")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[listenAddress], "0.0.0.0")
}

// HARM-004: locks log level parsing, including the silent debug fallback.
func TestParseLogLevel(t *testing.T) {
	is := is.New(t)

	is.Equal(parseLogLevel("debug"), slog.LevelDebug)
	is.Equal(parseLogLevel("info"), slog.LevelInfo)
	is.Equal(parseLogLevel("warn"), slog.LevelWarn)
	is.Equal(parseLogLevel("warning"), slog.LevelWarn)
	is.Equal(parseLogLevel("error"), slog.LevelError)
	is.Equal(parseLogLevel("bogus"), slog.LevelDebug)
}

// REV-015: exact-"true" toggles versus the strconv-parsed toggle use
// intentionally different interpretations. Tests target the production
// seams so a changed interpretation breaks them.
func TestBoolToggleInterpretations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		value        string
		exact        bool
		accessObject bool
	}{
		{"exact true", "true", true, true},
		{"uppercase TRUE", "TRUE", false, true},
		{"numeric 1", "1", false, true},
		{"false", "false", false, false},
		{"numeric 0", "0", false, false},
		{"empty", "", false, false},
		{"invalid", "bogus", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			flags := defaultFlags()
			flags[mqttEnabled] = tc.value
			flags[mqttInsecure] = tc.value
			flags[oauth2InsecureUrl] = tc.value
			flags[authzAccessObject] = tc.value

			is.Equal(mqttEnabledFlag(flags), tc.exact)
			is.Equal(mqttInsecureFlag(flags), tc.exact)
			is.Equal(oauthInsecureFlag(flags), tc.exact)
			is.Equal(accessObjectEnabled(flags), tc.accessObject)
		})
	}
}
