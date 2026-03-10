"""
Startup sequence:
  1. Validate environment variables
  2. Print pretty startup banner
  3. Start health HTTP server on HEALTH_PORT (default 8099)
     - GET /health → 503 + {"status":"starting"} until ready
     - GET /health → 200 + {"status":"ready"} once replicator is running
     - POST /dlq/replay → Trigger DLQ replay logic
  4. Wait for all DynamoDB Local instances to be healthy
  5. Run cfddb against each instance (idempotent schema deployment)
  6. Enable DynamoDB Streams on all GLOBAL_TABLES (cfddb doesn't do this)
  7. Signal supervisord to start the replicator
  8. Flip health status to ready
"""

import boto3
import json
import logging
import os
import subprocess
import threading
import time
import xmlrpc.client
import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [init] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
console = Console()


REGIONS = os.environ.get("REGIONS", "")
GLOBAL_TABLES = os.environ.get("GLOBAL_TABLES", "")

if not REGIONS and __name__ == "__main__":
    log.error("Missing required environment variable: REGIONS")
    sys.exit(1)

SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/schema/dynamodb.yaml")
CFN_PARAMETERS = os.environ.get("CFN_PARAMETERS", "AppEnv=local")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8099"))
REPLICATION_MODE = os.environ.get("REPLICATION_MODE", "latest")
DLQ_DIR = Path(os.environ.get("DLQ_DIR", "/tmp/dlq"))

WAIT_RETRIES = 40
WAIT_SLEEP = 2
VERSION = "1.2.0"
REPL_SENTINEL_ATTR = "__dynamoatlas_repl__"

deserializer = TypeDeserializer()
serializer = TypeSerializer()

_health_state = {
    "status": "starting",
    "started_at": datetime.now(timezone.utc).isoformat(),
    "regions": [],
    "tables": [],
    "checks": {},
}
_health_lock = threading.Lock()


def set_health_ready():
    with _health_lock:
        _health_state["status"] = "ready"
        _health_state["ready_at"] = datetime.now(timezone.utc).isoformat()


def update_health_check(name, status):
    with _health_lock:
        _health_state["checks"][name] = status


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/health", "/"):
            self.send_response(404)
            self.end_headers()
            return

        with _health_lock:
            state = dict(_health_state)
        is_ready = state["status"] == "ready"
        code = 200 if is_ready else 503

        body = json.dumps(state, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path == "/dlq/replay":
            result = self.replay_dlq()
            body = json.dumps(result, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def replay_dlq(self):
        log.info("API: Replaying DLQ.")
        if not DLQ_DIR.exists():
            return {"status": "no_dlq_dir"}

        files = sorted(DLQ_DIR.glob("*.json"))
        if not files:
            return {"status": "empty", "replayed_count": 0}

        success = 0
        failed = 0
        for f in files:
            try:
                entry = json.loads(f.read_text())
                if self.apply_dlq_entry(entry):
                    success += 1
                    f.unlink()
                else:
                    failed += 1
            except Exception as e:
                log.error(f"Failed to process DLQ file {f.name}: {e}")
                failed += 1

        return {"status": "complete", "replayed_count": success, "failed_count": failed}

    def apply_dlq_entry(self, entry):
        dest = entry["dest"]
        table_name = entry["table"]
        record = entry["record"]
        event = record.get("eventName")
        ddb_data = record.get("dynamodb", {})

        port = dest.split(":")[-1]
        client = make_client(port)

        try:
            if event in ("INSERT", "MODIFY"):
                item = {
                    k: deserializer.deserialize(v)
                    for k, v in ddb_data.get("NewImage", {}).items()
                }
                client.put_item(
                    TableName=table_name,
                    Item={
                        k: serializer.serialize(v)
                        for k, v in {**item, REPL_SENTINEL_ATTR: True}.items()
                    },
                )

                resp = client.describe_table(TableName=table_name)
                schema = resp["Table"]["KeySchema"]
                pk = next(k["AttributeName"] for k in schema if k["KeyType"] == "HASH")
                sk = next(
                    (k["AttributeName"] for k in schema if k["KeyType"] == "RANGE"),
                    None,
                )

                key = {pk: serializer.serialize(item[pk])}
                if sk and sk in item:
                    key[sk] = serializer.serialize(item[sk])
                client.update_item(
                    TableName=table_name,
                    Key=key,
                    UpdateExpression="REMOVE #s",
                    ExpressionAttributeNames={"#s": REPL_SENTINEL_ATTR},
                )
            elif event == "REMOVE":
                keys = {
                    k: deserializer.deserialize(v)
                    for k, v in ddb_data.get("Keys", {}).items()
                }
                client.delete_item(
                    TableName=table_name,
                    Key={k: serializer.serialize(v) for k, v in keys.items()},
                )
            return True
        except Exception as e:
            log.error(f"Replay apply failed: {e}")
            return False

    def log_message(self, fmt, *args):
        pass  # suppress default access logs


def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    t.start()
    log.info(f"Health server listening on :{HEALTH_PORT}/health")


def print_banner(region_entries, global_tables):
    # Create the main header table
    table = Table(show_header=False, box=None, padding=(0, 1))

    # Add Region entries
    for region, port in region_entries:
        table.add_row(
            f"[bold cyan]Region[/bold cyan]", f"{region} [dim]→[/dim] localhost:{port}"
        )

    # Add Global tables
    for table_name in global_tables:
        table.add_row(f"[bold magenta]Global Table[/bold magenta]", table_name)

    # Add remaining config
    table.add_row(f"[bold yellow]Schema[/bold yellow]", SCHEMA_PATH)
    table.add_row(f"[bold yellow]Repl Mode[/bold yellow]", REPLICATION_MODE.upper())
    table.add_row(
        f"[bold green]Health[/bold green]", f"http://localhost:{HEALTH_PORT}/health"
    )

    # Wrap it all in a cool panel
    panel = Panel(
        table,
        title="[bold blue]DynamoAtlas[/bold blue]",
        subtitle=f"[dim]v{VERSION}[/dim]",
        border_style="blue",
        expand=False,
    )

    console.print(panel)


def make_client(port: str):
    return boto3.client(
        "dynamodb",
        endpoint_url=f"http://localhost:{port}",
        region_name="us-east-1",
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )


def parse_regions() -> list[tuple[str, str]]:
    entries = []
    try:
        for entry in REGIONS.split(","):
            parts = entry.strip().split(":")
            if len(parts) != 2:
                raise ValueError(f"Invalid region entry: {entry}. Expected region:port")
            region, port = parts
            entries.append((region.strip(), port.strip()))
    except Exception as e:
        log.error(f"Failed to parse REGIONS: {e}")
        sys.exit(1)
    return entries


def parse_tables() -> list[str]:
    return [t.strip() for t in GLOBAL_TABLES.split(",") if t.strip()]


def start_instances(region_entries: list):
    """Start DynamoDB Local instances as background processes."""
    db_dir = Path("/dynamodb")
    jar_path = db_dir / "DynamoDBLocal.jar"
    lib_path = db_dir / "DynamoDBLocal_lib"

    for region, port in region_entries:
        cmd = [
            "java",
            f"-Djava.library.path={lib_path}",
            "-jar",
            str(jar_path),
            "-sharedDb",
            "-port",
            port,
        ]
        log.info(f"Starting DynamoDB Local for {region} on :{port}")
        # Start as background process. We don't wait for completion here.
        subprocess.Popen(
            cmd,
            cwd="/dynamodb",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def wait_for_instance(region: str, port: str):
    client = make_client(port)
    for attempt in range(1, WAIT_RETRIES + 1):
        try:
            client.list_tables()
            log.info(f"{region}:{port} is ready")
            return
        except Exception:
            log.info(f"Waiting for {region}:{port} ... ({attempt}/{WAIT_RETRIES})")
            time.sleep(WAIT_SLEEP)
    raise RuntimeError(
        f"DynamoDB Local :{port} ({region}) never became ready after {WAIT_RETRIES} attempts"
    )


def run_cfddb(region: str, port: str):
    if not os.path.exists(SCHEMA_PATH):
        raise FileNotFoundError(f"Schema file not found at {SCHEMA_PATH}")

    cmd = [
        "cfddb",
        "--template",
        SCHEMA_PATH,
        "--endpoint",
        f"http://localhost:{port}",
        "--region",
        region,
        "--parameters",
        CFN_PARAMETERS,
    ]
    log.info(f"Running cfddb → {region}:{port}")
    result = subprocess.run(cmd, input="y\n", capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"cfddb failed for {region}:{port}")
        log.error(result.stderr)
        raise RuntimeError(
            f"cfddb failed for {region}:{port} (exit {result.returncode})"
        )
    log.info(f"cfddb done for {region}")


def ensure_stream_enabled(client, region: str, table_name: str):
    """Enable NEW_AND_OLD_IMAGES stream — required for replication."""
    resp = client.describe_table(TableName=table_name)
    spec = resp["Table"].get("StreamSpecification", {})
    if spec.get("StreamEnabled") and spec.get("StreamViewType") == "NEW_AND_OLD_IMAGES":
        log.info(f"Stream already active: '{table_name}' ({region})")
        return
    client.update_table(
        TableName=table_name,
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )
    log.info(f"Stream enabled: '{table_name}' ({region})")


def start_replicator():
    try:
        server = xmlrpc.client.ServerProxy("http://localhost:9001/RPC2")
        server.supervisor.startProcess("replicator")
        log.info(f"Replicator started via supervisord")
    except Exception as e:
        log.error(f"Failed to start replicator via supervisord: {e}")
        raise


def main():
    region_entries = parse_regions()
    global_tables = parse_tables()

    if not global_tables:
        log.error("No GLOBAL_TABLES specified.")
        sys.exit(1)

    # Update health state with config info
    with _health_lock:
        _health_state["regions"] = [f"{r}:{p}" for r, p in region_entries]
        _health_state["tables"] = global_tables
    print_banner(region_entries, global_tables)

    # Health server (starts immediately, returns 503 until ready)
    start_health_server()

    try:
        log.info("[0/4] Starting DynamoDB Local instances.")
        start_instances(region_entries)

        log.info("[1/4] Waiting for DynamoDB Local instances.")
        for region, port in region_entries:
            wait_for_instance(region, port)
        update_health_check("instances_ready", True)

        log.info("[2/4] Deploying schema via cfddb.")
        for region, port in region_entries:
            run_cfddb(region, port)
        update_health_check("schema_deployed", True)

        log.info("[3/4] Enabling streams on global tables.")
        valid_tables = []
        for region, port in region_entries:
            client = make_client(port)
            existing_tables = set(client.list_tables()["TableNames"])
            for table in global_tables:
                if table not in existing_tables:
                    log.warning(
                        f"Table '{table}' not found in region-port {region}:{port}. Skipping replication for this table."
                    )
                    continue

                try:
                    ensure_stream_enabled(client, region, table)
                    if table not in valid_tables:
                        valid_tables.append(table)
                except Exception as e:
                    log.error(f"Failed to enable stream for '{table}' ({region}): {e}")

        # Update global list to only include tables that actually exist
        global_tables[:] = valid_tables
        _health_state["tables"] = global_tables

        update_health_check("streams_enabled", True)

        log.info("[4/4] Starting replicator")
        start_replicator()
        update_health_check("replicator_started", True)
        set_health_ready()
        log.info("DynamoAtlas is ready")

    except Exception as e:
        log.error(f"Initialization failed: {e}")
        update_health_check("error", str(e))
        while True:
            time.sleep(60)


if __name__ == "__main__":
    main()
