#!/bin/bash
set -e

# Check if CONFIG_PATH is set
if [ -z "$CONFIG_PATH" ]; then
    echo "ERROR: CONFIG_PATH environment variable must be set"
    echo "Please set CONFIG_PATH to the path of your configuration file"
    echo "Example: -e CONFIG_PATH=/app/config/config.yaml"
    exit 1
fi

# Check if config file exists at specified path
if [ ! -f "$CONFIG_PATH" ]; then
    echo "ERROR: Configuration file not found at $CONFIG_PATH"
    echo "Please make sure to mount your configuration file to the specified path"
    echo "Example: -v /path/to/your/config.yaml:$CONFIG_PATH"
    exit 1
fi

# Start the application with the specified config
exec java $JAVA_OPTS -jar /app/app.jar "$CONFIG_PATH"