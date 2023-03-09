# iot-events

## VSCode

launch.json

```json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
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
                "--notifications=${workspaceFolder}/assets/config/notifications.yaml"
            ]
        }
    ]
}
```
