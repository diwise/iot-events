# iot-events

## VSCode

launch.json

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch Package",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "program": "${workspaceFolder}/cmd/iot-events",
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

.

# Configuration

## Faktisk konfiguration (kod ar facit, HARM-002)
Precedens: default < miljovariabel < CLI-flagga. RabbitMQ konfigureras i ovrigt via `messaging.LoadConfiguration`.

| Variabel | Default | Notering |
| --- | --- | --- |
| `LISTEN_ADDRESS` | `0.0.0.0` | Bind-adress for bada webbservrarna |
| `SERVICE_PORT` | `8080` | Publik server (`/api/v0/measurements`, `/api/v1/measurements`, `/openapi.yaml`, `/docs`) |
| `CONTROL_PORT` | `8000` | Kontrollserver: pprof, liveness, readiness-stubbar (`rabbitmq`, `mqtt`, `timescale`) som returnerar OK |
| `RABBITMQ_TOPIC` | `#` | Topicfilter vid registrering; `message.accepted` far dessutom en dedikerad lagringshandler |
| `POSTGRES_HOST` | (tom) |  |
| `POSTGRES_PORT` | `5432` |  |
| `POSTGRES_DBNAME` | `diwise` |  |
| `POSTGRES_USER` | (tom) |  |
| `POSTGRES_PASSWORD` | (tom) |  |
| `POSTGRES_SSLMODE` | `disable` |  |
| `MQTT_ENABLED` | `false` | Device-management-klienten aktiveras endast nar MQTT ar aktiverat |
| `MQTT_BROKER_URL` | `tcp://localhost:1883` |  |
| `MQTT_USER` | (tom) |  |
| `MQTT_PASSWORD` | (tom) |  |
| `MQTT_CLIENT_ID` | `iot-events` |  |
| `MQTT_INSECURE` | `true` |  |
| `MQTT_PREFIX` | `devices/` |  |
| `MQTT_IDENTIFIER` | `deviceID` |  |
| `DEV_MGMT_URL` | `http://iot-device-mgmt` |  |
| `OAUTH2_TOKEN_URL` | (tom) |  |
| `OAUTH2_CLIENT_ID` | (tom) |  |
| `OAUTH2_CLIENT_SECRET` | (tom) |  |
| `OAUTH2_REALM_INSECURE` | `false` |  |
| `AUTHZ_ACCESS_OBJECT_ENABLED` | `false` | Switches between the `tenants` and `access` result models |
| `LOG_LEVEL` | `debug` |  |
| `RABBITMQ_HOST` | (tom, kravs om inte avstangd) | Se `messaging.LoadConfiguration` |
| `RABBITMQ_PORT` | `5672` |  |
| `RABBITMQ_VHOST` | `/` |  |
| `RABBITMQ_USER` | `user` |  |
| `RABBITMQ_PASS` | `bitnami` |  |
| `RABBITMQ_DISABLED` | `false` |  |
| `RABBITMQ_INIT_TIMEOUT` | `10` | Sekunder |
| `POSTGRES_MAX_CONNS` | `10` |  |
| `POSTGRES_MIN_CONNS` | `2` |  |
| `POSTGRES_MAX_CONN_LIFETIME` | `30m` |  |
| `POSTGRES_MAX_CONN_IDLE_TIME` | `5m` |  |
| `POSTGRES_HEALTH_CHECK_PERIOD` | `30s` |  |

Health paths pa kontrollservern (`CONTROL_PORT`): `/health`, `/healthz`, `/livez`, `/readyz`, `/readyz/{check}`.

Externa Kubernetes- och Compose-definitioner finns inte i detta repo och ar darfor inte inventerade har.

Alla booleska toggles (`MQTT_ENABLED`, `MQTT_INSECURE`, `OAUTH2_REALM_INSECURE`, `AUTHZ_ACCESS_OBJECT_ENABLED`) tolkas med strconv-semantik; ogiltiga varden blir `false`.

## CLI flags
 - `cloudevents` - Configuration file for cloud events (default `/opt/diwise/config/cloudevents.yaml`)
 - `policies` - An authorization policy file (default `/opt/diwise/config/authz.rego`)
 - `authz-access-object` - Enable the access-object authorization policy result model
 - `metadata` - A CSV file with initial metadata (default `/opt/diwise/config/metadata.csv`; saknad fil tolereras)
 - `loglevel` - Set the log level (overrides `LOG_LEVEL`)

## Configuration files
 - `cloudevents.yaml` - Required at startup, defines CloudEvents subscribers.
 - `authz.rego` - Required at startup, OPA policy.
 - `metadata.csv` - Optional seed metadata; startup fortsatter utan den.

# API

Alla routes kraver scope `measurements.read` (bearer-JWT via OPA-policy). Hela versionstradet ligger bakom auth, inklusive okanda paths.

| Metod | Path | Notering |
| --- | --- | --- |
| `GET` | `/api/v0/measurements` | Fragestallning utan metadatastod |
| `GET` | `/api/v0/measurements/{deviceID}` | Matningar for enhet; `?latest=true` ger senaste, `?urn=` maste vara `urn:oma:lwm2m...` |
| `GET` | `/api/v1/measurements` | Fragestallning med metadatastod via `metadata[nyckel]`; ger 403 utan tillatna tenants |
| `GET` | `/openapi.yaml` | OpenAPI 3.0-spec (se `assets/docs/openapi.yaml`) |
| `GET` | `/docs` | Redoc-sida for specen |

v0- och v1-paths, queryparametrar och svarskoder i `assets/docs/openapi.yaml` motsvarar faktiska handlers i `internal/presentation/api`; specen andras inte utan runtimeandring.

# Lifecycle och beroenden

Tjansten kor via `servicerunner`: `OnInit` (storage, messenger, mediator, MQTT, device-management), `OnStarting` (mediator, CloudEvents, MQTT-publisher, topichandlers for `RABBITMQ_TOPIC` samt dedikerad `message.accepted`-lagringshandler), `OnShutdown` (stoppar inflode, dränerar antagna handleranrop inom 10 s, stanger storage).

Readiness-stubbar (`rabbitmq`, `mqtt`, `timescale`) returnerar alltid OK och anropar inga beroenden.

Externa beroenden: PostgreSQL/TimescaleDB, RabbitMQ (kan stangas av med `RABBITMQ_DISABLED`), MQTT-broker (valfri via `MQTT_ENABLED`; aktiverar ocksa device-management-klienten), `iot-device-mgmt` for enhetsuppslag, samt CloudEvents-mottagare enligt `cloudevents.yaml`.

# Verifiering

```bash
gofmt -l cmd/ internal/
go test -count=1 ./...
go vet ./...
go build ./...
docker build -f deployments/Dockerfile .
```
