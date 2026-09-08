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
| `SERVICE_PORT` | `8080` | Publik server (`/api/v0/measurements`, `/api/v1/measurements`, `/openapi.yaml`, `/docs`) |
| `CONTROL_PORT` | `8000` | Kontrollserver: pprof, liveness, readiness (`rabbitmq`-stubb, `mqtt`, `timescale`-ping) |
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

`LISTEN_ADDRESS` ar hardkodat till `0.0.0.0` i flaggmodellen och kan varken styras via env eller CLI i nulaget.

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
