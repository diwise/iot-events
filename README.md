# iot-events

IoT event ingestion and query service for DIWISE. It receives messages from the messaging bus, fans them out through an internal mediator, stores measurements in Postgres/TimescaleDB, emits CloudEvents, and optionally republishes value/status updates to MQTT.

## Architecture

Core components (see `cmd/iot-events/main.go`):

- Messaging input: `messaging-golang` subscriber, topic defaults to `#`. Incoming messages are decoded and pushed into the internal mediator.
- Mediator: fan-out bus that delivers messages to multiple subscribers concurrently.
- CloudEvents publisher: configurable subscribers and endpoints in `assets/config/cloudevents.yaml`.
- Storage: Postgres/TimescaleDB, tables `events_measurements` and `events_measurements_metadata`.
- HTTP API: `/api/v0` and `/api/v1` endpoints, guarded by OPA Rego policies.
- MQTT publisher (optional): publishes mapped sensor values and status updates to MQTT topics, enriched with device metadata from device-management.

Supporting services:

- Device Management API: used to resolve device metadata for MQTT publications.
- OPA policy evaluation: authz in `assets/config/authz.rego`.
- Observability: OpenTelemetry tracing and structured JSON logs.
- Control server: liveness/readiness probes and pprof enabled via service-chassis.

## Data Flow

```
Messaging bus (RabbitMQ topic) --> application handler --> mediator
                                                   |-> CloudEvents (config-driven)
                                                   |-> MQTT publisher (message.accepted, device-status)
                                                   |-> Measurement storage (message.accepted)

HTTP API (query) --> storage (read-only)
```

Details:

- `application.NewMessageHandler` parses incoming messaging-bus events, extracts tenant, and publishes a mediator message with `Type = topic`.
- `measurements.NewMessageAcceptedHandler` listens to `message.accepted`, parses the SenML pack, and persists measurements.
- `cloudevents.CloudEvents` subscribes based on `assets/config/cloudevents.yaml` and forwards matching messages as CloudEvents.
- `mqtt.Start` subscribes to `message.accepted` and `device-status`, enriches with device metadata, maps resource IDs to friendly names, and publishes to MQTT.
- API handlers query data from the database and enforce tenant access via OPA policies.

## HTTP API

Public server listens on `SERVICE_PORT` (default `8080`).

- `GET /api/v0/measurements`
  - Query by `id` and additional filters (`timerel`, `timeat`, `endtimeat`, `limit`, `offset`, `name`, `latest`).
- `GET /api/v0/measurements/{deviceID}`
  - Optional `urn`, optional `latest=true`.
- `GET /api/v1/measurements`
  - Same as v0, plus metadata filters like `metadata[key]=value`.

Authorization:

- Requires `Authorization: Bearer <token>`.
- Policy is evaluated by OPA (`assets/config/authz.rego`).
- Allowed tenants are extracted from policy response and used to scope queries.

## Configuration

### Command-line flags

- `--cloudevents` path to CloudEvents config (default `/opt/diwise/config/cloudevents.yaml`).
- `--policies` path to OPA policy file (default `/opt/diwise/config/authz.rego`).
- `--metadata` path to metadata CSV file (default `/opt/diwise/config/metadata.csv`).

### Environment variables

Server:

- `SERVICE_PORT` (default `8080`)
- `CONTROL_PORT` (default `8000`)

Messaging bus:

- `RABBITMQ_TOPIC` (default `#`)
- Additional messaging configuration is loaded by `messaging-golang` via its own env vars.

Database:

- `POSTGRES_HOST`
- `POSTGRES_PORT` (default `5432`)
- `POSTGRES_DBNAME` (default `diwise`)
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_SSLMODE` (default `disable`)

MQTT (optional):

- `MQTT_ENABLED` (default `false`)
- `MQTT_BROKER_URL` (default `tcp://localhost:1883`)
- `MQTT_USER`
- `MQTT_PASSWORD`
- `MQTT_CLIENT_ID` (default `iot-events`)
- `MQTT_INSECURE` (default `true`, skips TLS verification)
- `MQTT_PREFIX` (default `devices/`)
- `MQTT_IDENTIFIER` (default `deviceID`, can be `sensorID` or `name`)

Device management / OAuth2:

- `DEV_MGMT_URL` (default `http://iot-device-mgmt`)
- `OAUTH2_TOKEN_URL`
- `OAUTH2_CLIENT_ID`
- `OAUTH2_CLIENT_SECRET`
- `OAUTH2_REALM_INSECURE` (default `false`, skips TLS verification)

## CloudEvents Configuration

`assets/config/cloudevents.yaml`:

```
subscribers:
  - id: <string>
    name: <string>
    type: <message type / topic>
    endpoint: <http url>
    source: <string>
    eventType: <string>
    tenants: [default, ...]
    entities:
      - idPattern: <regexp>
```

Subscribers match by message type (topic), tenant, and optional entity ID regex.

## Metadata CSV

`assets/config/metadata.csv` is loaded at startup and seeded into `events_measurements_metadata`.

Format:

```
id;key;value
<measurement_id>;<key>;<value>
```

`id` may include device/object path (e.g. `<deviceID>/...`), which is also used to infer `deviceID`.

## Development

Run the service locally:

```bash
go run ./cmd/iot-events \
  --cloudevents ./assets/config/cloudevents.yaml \
  --policies ./assets/config/authz.rego \
  --metadata ./assets/config/metadata.csv
```

Start a local TimescaleDB:

```bash
docker compose -f deployments/docker-compose.yaml up
```

Build the container:

```bash
docker build -f deployments/Dockerfile .
```

## VSCode

Example `launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Launch Package",
      "type": "go",
      "request": "launch",
      "mode": "auto",
      "program": "${workspaceFolder}/cmd/iot-events/main.go",
      "env": {
        "RABBITMQ_DISABLED": "true"
      },
      "args": [
        "--cloudevents=${workspaceFolder}/assets/config/cloudevents.yaml"
      ]
    }
  ]
}
```
