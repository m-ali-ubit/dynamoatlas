#!/bin/bash
set -e

mkdir -p /data /var/log /tmp/dlq

echo "Starting DynamoAtlas via Supervisord."
exec supervisord -n -c /app/supervisord.conf
