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
            "program": "${workspaceFolder}/cmd/iot-events/main.go",
            "env": {
                "RABBITMQ_DISABLED": "true"
            },
            "args": [
                "--notifications=${workspaceFolder}/assets/config/cloudevents.yaml"
            ]
        }
    ]
}
```


