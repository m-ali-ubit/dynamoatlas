# DynamoAtlas

**Local DynamoDB Multi Region Emulator — one container, any number of regions, full bidirectional replication.**

DynamoAtlas runs multiple `dynamodb-local` instances inside a single Docker container and keeps them in sync using DynamoDB Streams,
mimicking the behavior of [AWS Global Tables](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html) for local development.
It uses [cfddb](https://github.com/m-ali-ubit/cfddb) to deploy your existing CloudFormation schema to every instance,
no changes to your schema file required.

---

## Why does this exist?

`dynamodb-local` (the official Docker image from AWS) has no concept of regions.
Passing `region_name="eu-central-1"` to your boto3 client makes no difference, both hit the same single data store.
This is fine for single-region apps, but breaks down the moment you're building against multi region architecture,
where the same table may exist in multiple regions and writes in one region replicate to all others automatically.

DynamoAtlas solves this by:
- Spinning up one `dynamodb-local` process per region, each on its own port
- Running [cfddb](https://pypi.org/project/cfddb) to deploy your CloudFormation YAML schema to each instance
- Enabling DynamoDB Streams on global tables
- Running a replication engine that polls each stream and applies changes to all other instances bidirectionally

Your app code just switches the endpoint URL based on region, everything else behaves like real Global Tables.

---

## How it works

```
┌────────────────────────────────────────────────────────────┐
│                     DynamoAtlas container                  │
│  supervisord                                               │
│  ├── dynamodb-local :8000  (eu-central-1)                  │
│  ├── dynamodb-local :8001  (us-east-1)                     │
│  ├── ...                                                   │
│  ├── init.py                                               │
│  │     1. Waits for all instances to be healthy            │
│  │     2. Runs cfddb against each instance (dynamodb.yaml) │
│  │     3. Enables DynamoDB Streams on global tables        │
│  │     4. Signals replicator to start                      │
│  │     5. Marks health endpoint as ready                   │
│  │                                                         │
│  ├── replicator.py                                         │
│  │     - Shared ThreadPoolExecutor (scalable workers)      │
│  │     - Persists shard state to /data/shard_state.json    │
│  │     - Polls DynamoDB Streams on each instance           │
│  │     - Applies changes to all other instances            │
│  │     - Handles shard rotation, lag detection, DLQ        │
│  │                                                         │
│  └── health server :8099                                   │
│        GET /health → 503 (starting) | 200 (ready)          │
└────────────────────────────────────────────────────────────┘
```

---

## Quick start

```yaml
# docker-compose.yml
services:
  dynamoatlas:
    image: usermalikhan/dynamoatlas:latest
    ports:
      - "8000:8000"   # eu-central-1
      - "8001:8001"   # us-east-1
      - "8099:8099"   # health endpoint
    environment:
      - REGIONS=eu-central-1:8000,us-east-1:8001
      - GLOBAL_TABLES=Projects
      - CFN_PARAMETERS=AppEnv=local
    volumes:
      - ./dynamodb.yaml:/schema/dynamodb.yaml
      - dynamoatlas_data:/data

volumes:
  dynamoatlas_data:
```

```bash
docker compose up dynamoatlas
```

Wait for the health endpoint:
```bash
curl http://localhost:8099/health
# {"status": "ready", ...}
```

---

## Environment variables

| Variable                | Required | Default                  | Description                                                                            |
|-------------------------|----------|--------------------------|----------------------------------------------------------------------------------------|
| `REGIONS`               | Yes      | —                        | Comma-separated `region:port` pairs. e.g. `eu-central-1:8000,us-east-1:8001`           |
| `GLOBAL_TABLES`         | No       | —                        | Comma-separated table names to replicate. e.g. `Projects,Users`                        |
| `CFN_PARAMETERS`        | No       | `AppEnv=local`           | Passed to `cfddb --parameters`. Should match your existing setup command.              |
| `SCHEMA_PATH`           | No       | `/schema/dynamodb.yaml`  | Path inside the container to your CloudFormation schema.                               |
| `REPLICATION_MODE`      | No       | `latest`                 | `latest` — only new writes. `full` — replay all existing data on start (TRIM_HORIZON). |
| `POLL_INTERVAL`         | No       | `1`                      | Replication poll interval in seconds.                                                  |
| `LAG_WARN_SECONDS`      | No       | `10`                     | Log a warning if replication lag exceeds this threshold.                               |
| `SHARD_REFRESH_SECONDS` | No       | `30`                     | How often to check for new stream shards (handles shard rotation).                     |
| `HEALTH_PORT`           | No       | `8099`                   | Port for the health HTTP server.                                                       |
| `DLQ_DIR`               | No       | `/tmp/dlq`               | Directory where failed replication records are written for later replay.               |
| `STATE_FILE`            | No       | `/data/shard_state.json` | Path to the persistence file for shard sequence numbers.                               |
| `MAX_WORKERS`           | No       | `50`                     | Maximum concurrent workers in the replication thread pool.                             |

---

## Adding more regions

Just extend `REGIONS` and expose the port, no other changes:

```yaml
environment:
  - REGIONS=eu-central-1:8000,us-east-1:8001,ap-southeast-1:8002
ports:
  - "8000:8000"
  - "8001:8001"
  - "8002:8002"
```

Replication scales to N regions automatically, a central thread pool manages workers across all tables and directions.

---

## Adding more global tables

```yaml
environment:
  - GLOBAL_TABLES=Projects,Orders,Users
```

All listed tables must exist in your `dynamodb.yaml`. Each gets streams enabled and is replicated bidirectionally across all regions.

---

## Pointing your app at the right region

Switch the DynamoDB endpoint URL based on region. The region value itself doesn't matter to DynamoDB Local, only the port does.

```python
# Python / boto3
import os, boto3

REGION_ENDPOINTS = {
    "eu-central-1": "http://localhost:8000",
    "us-east-1":    "http://localhost:8001",
}

region   = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")
endpoint = REGION_ENDPOINTS[region]

client = boto3.client(
    "dynamodb",
    endpoint_url=endpoint,
    region_name=region,
    aws_access_key_id="local",
    aws_secret_access_key="local",
)
```

```typescript
// TypeScript / AWS SDK v3
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const REGION_ENDPOINTS: Record<string, string> = {
  "eu-central-1": "http://localhost:8000",
  "us-east-1":    "http://localhost:8001",
};

const region   = process.env.AWS_DEFAULT_REGION ?? "eu-central-1";
const endpoint = REGION_ENDPOINTS[region];

const client = new DynamoDBClient({
  region,
  endpoint,
  credentials: { accessKeyId: "local", secretAccessKey: "local" },
});
```

---

## Waiting for DynamoAtlas in docker-compose

The HEALTHCHECK is baked into the image. Any service that needs DynamoDB to be fully initialized before starting can use:

```yaml
services:
  your-app:
    image: your-app:latest
    depends_on:
      dynamoatlas:
        condition: service_healthy   # waits for /health → {"status":"ready"}
```

---

## Health endpoint

```
GET http://localhost:8099/health
```

Returns `503` while initializing, `200` once the replicator is running:

```json
{
  "status": "ready",
  "started_at": "2024-01-15T10:00:00+00:00",
  "ready_at":   "2024-01-15T10:00:08+00:00",
  "regions": ["eu-central-1:8000", "us-east-1:8001"],
  "tables":  ["Projects", "Users"]
}
```

---

## Replication internals

### Deduplication (no flag leakage)

The replicator needs to tell the difference between a write from your app and a write it made itself (to avoid an infinite replication loop).
The naive approach is to write a `_repl: true` attribute onto every replicated item, but this leaks a spurious attribute
into every item that doesn't exist in real DynamoDB.

DynamoAtlas uses a two-phase write instead:

1. `put_item` with a sentinel attribute `__dynamoatlas_repl__` included in the item
2. Immediately `update_item` to remove the sentinel

When the destination instance sees this write in its own stream, the `NewImage` contains `__dynamoatlas_repl__`,
so the replicator skips it. By the time any application code reads the item, the sentinel has already been removed. Items are always clean.

### GSI replication

DynamoDB Local updates GSI entries automatically when you `put_item` with attributes that match a GSI key schema,
same as real DynamoDB. DynamoAtlas fetches and caches the full key schema (base table + all GSIs + LSIs) for each table on startup,
so it can log which GSI key attributes were involved in each replicated write and detect any schema mismatches early.

### Shard rotation

DynamoDB Streams shards are not permanent, they close and new ones open over time.
DynamoAtlas re-describes each stream every `SHARD_REFRESH_SECONDS` (default: 30s) and picks up any new shards,
so long-running containers don't miss events after a shard rotation.

### Replication modes

| Mode               | Iterator type  | Use case                                                          |
|--------------------|----------------|-------------------------------------------------------------------|
| `latest` (default) | `LATEST`       | Normal dev, only replicate writes made after startup              |
| `full`             | `TRIM_HORIZON` | Sync pre-existing data, replay all records in the stream on start |

### Persistence

Replication state is persisted to disk. Every time a batch of records is processed, the replicator updates the last seen `SequenceNumber` for that shard in `/data/shard_state.json`.

If the container restarts, DynamoAtlas will:
1. Load the state file.
2. For each shard, resume replication from the stored `SequenceNumber` (using `AFTER_SEQUENCE_NUMBER`).
3. If no state is found for a shard, it falls back to your configured `REPLICATION_MODE`.

This ensures that replication survives container upgrades or Docker host reboots without missing data.

---

## Dead Letter Queue

When a record fails to replicate (e.g. a transient write error), it's written as a JSON file to `DLQ_DIR` (default `/tmp/dlq`) rather than silently dropped.

Each file contains the full stream record, source/destination labels, table name, timestamp, and the error message.

### Inspecting the DLQ

```bash
docker exec dynamoatlas ls /tmp/dlq/
# 20240115T100512123456_eu-central-1-8000_Project.json

docker exec dynamoatlas cat /tmp/dlq/20240115T100512123456_eu-central-1-8000_Project.json
```

### Replaying failed records

#### Option A: Integrated API (Recommended)
Trigger a replay via a simple POST request to the health server:
```bash
curl -X POST http://localhost:8099/dlq/replay
# {"status": "complete", "replayed_count": 5, "failed_count": 0}
```

#### Option B: CLI tool
```bash
# Replay all DLQ entries
docker exec dynamoatlas python3 /app/dlq_replay.py

# Dry run, see what would be replayed
docker exec dynamoatlas python3 /app/dlq_replay.py --dry-run
```

# Replay only a specific table
docker exec dynamoatlas python3 /app/dlq_replay.py --table Project

# Replay only records targeting a specific destination
docker exec dynamoatlas python3 /app/dlq_replay.py --dest eu-central-1:8000

# Replay but keep the DLQ files (default: delete on success)
docker exec dynamoatlas python3 /app/dlq_replay.py --keep
```

Mount the DLQ dir as a volume to inspect files from the host:

```yaml
volumes:
  - ./local-dlq:/tmp/dlq
```

---

## Logs

```bash
# All processes
docker logs dynamoatlas

# Individual process logs (inside container)
docker exec dynamoatlas cat /var/log/init.log
docker exec dynamoatlas cat /var/log/replicator.log
docker exec dynamoatlas cat /var/log/dynamodb-eu-central-1.log
docker exec dynamoatlas cat /var/log/dynamodb-us-east-1.log
```

---

## Building

```bash
# Build with default DynamoDB Local version
docker build -t dynamoatlas:latest .
```

---

## Limitations

- **Last-write-wins** — concurrent writes to the same item in different regions will resolve by last write,
same as real Global Tables. There is no conflict detection.
- **No TTL replication** — DynamoDB Local does not enforce TTL expiry, and TTL deletes are not surfaced in streams.
TTL fields are replicated as regular attributes.
- **Streams retention** — DynamoDB Streams retains records for 24 hours. If the container is stopped for longer than that,
`REPLICATION_MODE=full` can be used to resync.

---

## License

Apache License 2.0
