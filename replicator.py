"""
Advanced bidirectional replication between DynamoDB Local instances.

Phase 2 Features (Refined):
  - ThreadPoolExecutor: managed worker pool for scalable polling.
  - State Persistence: saves shard SequenceNumbers to /data/shard_state.json.
  - Resource Efficiency: task-based polling logic.
  - [Restored] Shard Pagination: handles streams with >100 shards.
  - [Restored] Lag Detection: warns if replication falls behind LAG_WARN_SECONDS.
  - [Restored] GSI Awareness: debug logs for projected attribute replication.
"""

import boto3
import json
import logging
import os
import signal
import threading
import time
import concurrent.futures

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [replicator] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REGIONS = os.environ.get("REGIONS", "")
GLOBAL_TABLES = os.environ.get("GLOBAL_TABLES", "")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1"))
REPLICATION_MODE = os.environ.get("REPLICATION_MODE", "latest").lower()
LAG_WARN_SECONDS = float(os.environ.get("LAG_WARN_SECONDS", "10"))
SHARD_REFRESH_SEC = float(os.environ.get("SHARD_REFRESH_SECONDS", "30"))
DLQ_DIR = Path(os.environ.get("DLQ_DIR", "/tmp/dlq"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "/data/shard_state.json"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "50"))

REPL_SENTINEL_ATTR = "__dynamoatlas_repl__"

deserializer = TypeDeserializer()
serializer = TypeSerializer()

_table_schema_cache = {}
_schema_cache_lock = threading.Lock()
_shutdown = threading.Event()

_state_cache = {}
_state_lock = threading.Lock()


def load_shard_state():
    global _state_cache
    if STATE_FILE.exists():
        try:
            _state_cache = json.loads(STATE_FILE.read_text())
            log.info(
                f"Loaded shard state from {STATE_FILE} ({len(_state_cache)} entries)"
            )
        except Exception as e:
            log.warning(f"Failed to load shard state: {e}")
    return _state_cache


def save_shard_state():
    with _state_lock:
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps(_state_cache, indent=2))
        except Exception as e:
            log.warning(f"Failed to save shard state: {e}")


def update_shard_sequence(src_label, table, shard_id, seq_num):
    key = f"{src_label}:{table}:{shard_id}"
    with _state_lock:
        _state_cache[key] = seq_num


def handle_signal(signum, frame):
    log.info("Shutdown signal received — saving state and stopping.")
    _shutdown.set()


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def parse_regions() -> list[list[str]]:
    return [entry.strip().split(":") for entry in REGIONS.split(",")]


def parse_tables() -> list[str]:
    return [t.strip() for t in GLOBAL_TABLES.split(",") if t.strip()]


def make_ddb_client(port: str):
    return boto3.client(
        "dynamodb",
        endpoint_url=f"http://localhost:{port}",
        region_name="us-east-1",
        aws_access_key_id="fake",
        aws_secret_access_key="fake",
    )


def make_streams_client(port: str):
    return boto3.client(
        "dynamodbstreams",
        endpoint_url=f"http://localhost:{port}",
        region_name="us-east-1",
        aws_access_key_id="fake",
        aws_secret_access_key="fake",
    )


def fetch_table_schema(ddb_client, table_name: str, endpoint: str) -> dict:
    cache_key = (endpoint, table_name)
    with _schema_cache_lock:
        if cache_key in _table_schema_cache:
            return _table_schema_cache[cache_key]

    resp = ddb_client.describe_table(TableName=table_name)
    table = resp["Table"]
    key_schema = {k["AttributeName"]: k["KeyType"] for k in table["KeySchema"]}
    pk = next(k for k, t in key_schema.items() if t == "HASH")
    sk = next((k for k, t in key_schema.items() if t == "RANGE"), None)

    key_attrs = set(key_schema.keys())
    for gsi in table.get("GlobalSecondaryIndexes", []):
        for key in gsi["KeySchema"]:
            key_attrs.add(key["AttributeName"])
    for lsi in table.get("LocalSecondaryIndexes", []):
        for key in lsi["KeySchema"]:
            key_attrs.add(key["AttributeName"])

    schema = {"pk": pk, "sk": sk, "key_attrs": key_attrs}
    with _schema_cache_lock:
        _table_schema_cache[cache_key] = schema

    log.debug(
        f"Schema cached for '{table_name}': pk={pk}, sk={sk}, "
        f"gsi_key_attrs={key_attrs - {pk, sk}}"
    )
    return schema


def get_stream_arn(ddb_client, table_name: str) -> str:
    resp = ddb_client.describe_table(TableName=table_name)
    arn = resp["Table"].get("LatestStreamArn")
    if not arn:
        raise RuntimeError(f"No stream on '{table_name}'")
    return arn


def get_all_shards(streams_client, stream_arn: str) -> list[dict]:
    """Return all shards for a stream, handling pagination."""
    shards = []
    kwargs: dict = {"StreamArn": stream_arn}
    while True:
        resp = streams_client.describe_stream(**kwargs)
        shards += resp["StreamDescription"]["Shards"]
        last = resp["StreamDescription"].get("LastEvaluatedShardId")
        if not last:
            break
        kwargs["ExclusiveStartShardId"] = last
    return shards


def build_shard_iterators(
    streams_client,
    stream_arn: str,
    src_label: str,
    table_name: str,
    known_shard_ids: set,
) -> dict:
    shards = get_all_shards(streams_client, stream_arn)
    new_iterators = {}

    for shard in shards:
        shard_id = shard["ShardId"]
        if shard_id in known_shard_ids:
            continue

        state_key = f"{src_label}:{table_name}:{shard_id}"
        last_seq = _state_cache.get(state_key)

        try:
            if last_seq:
                log.info(f"Resuming shard {shard_id[:12]}... from {last_seq[:10]}...")
                resp_iter = streams_client.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    SequenceNumber=last_seq,
                )
            else:
                iterator_type = (
                    "TRIM_HORIZON" if REPLICATION_MODE == "full" else "LATEST"
                )
                resp_iter = streams_client.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard_id,
                    ShardIteratorType=iterator_type,
                )
            new_iterators[shard_id] = resp_iter["ShardIterator"]
        except Exception as e:
            log.warning(f"Could not get iterator for shard {shard_id}: {e}")

    return new_iterators


def deserialize_item(raw: dict) -> dict:
    return {k: deserializer.deserialize(v) for k, v in raw.items()}


def serialize_item(item: dict) -> dict:
    return {k: serializer.serialize(v) for k, v in item.items()}


def check_lag(record: dict, src_label: str, table_name: str):
    approx_ts = record.get("dynamodb", {}).get("ApproximateCreationDateTime")
    if approx_ts is None:
        return
    try:
        event_time = datetime.fromtimestamp(float(approx_ts), tz=timezone.utc)
        lag = (datetime.now(timezone.utc) - event_time).total_seconds()
        if lag > LAG_WARN_SECONDS:
            log.warning(
                f"Lag {lag:.1f}s on {src_label} '{table_name}' (threshold: {LAG_WARN_SECONDS}s)"
            )
    except:
        pass


def write_to_dlq(src: str, dest: str, table: str, record: dict, error: str):
    DLQ_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    payload = {
        "timestamp": ts,
        "src": src,
        "dest": dest,
        "table": table,
        "error": error,
        "record": record,
    }
    try:
        (DLQ_DIR / f"{ts}_{src.replace(':', '-')}_{table}.json").write_text(
            json.dumps(payload, indent=2, default=str)
        )
    except Exception as e:
        log.error(f"DLQ write failed: {e}")


def apply_batch_removes(dest_client, table_name: str, records: list):
    for i in range(0, len(records), 25):
        batch = records[i : i + 25]
        delete_requests = [
            {
                "DeleteRequest": {
                    "Key": serialize_item(deserialize_item(r["dynamodb"]["Keys"]))
                }
            }
            for r in batch
        ]
        try:
            dest_client.batch_write_item(RequestItems={table_name: delete_requests})
        except Exception as e:
            log.error(f"Batch delete failed for '{table_name}': {e}")
            for r in batch:
                try:
                    dest_client.delete_item(
                        TableName=table_name,
                        Key=serialize_item(deserialize_item(r["dynamodb"]["Keys"])),
                    )
                except:
                    pass


def apply_record(dest_client, table_name: str, dest_endpoint: str, record: dict):
    if record["eventName"] not in ("INSERT", "MODIFY"):
        return
    new_image = record["dynamodb"].get("NewImage", {})
    if REPL_SENTINEL_ATTR in new_image:
        return

    schema = fetch_table_schema(dest_client, table_name, dest_endpoint)
    item = deserialize_item(new_image)
    dest_client.put_item(
        TableName=table_name, Item=serialize_item({**item, REPL_SENTINEL_ATTR: True})
    )

    key = {schema["pk"]: serializer.serialize(item[schema["pk"]])}
    if schema["sk"] and schema["sk"] in item:
        key[schema["sk"]] = serializer.serialize(item[schema["sk"]])
    try:
        dest_client.update_item(
            TableName=table_name,
            Key=key,
            UpdateExpression="REMOVE #s",
            ExpressionAttributeNames={"#s": REPL_SENTINEL_ATTR},
        )
    except Exception as e:
        log.warning(f"Sentinel removal failed: {e}")

    # Log if any GSI key attributes were in this write
    gsi_attrs = schema["key_attrs"] - {schema["pk"], schema["sk"]}
    touched_gsi = gsi_attrs & set(item.keys())
    if touched_gsi:
        log.debug(f"GSI key attrs replicated for '{table_name}': {touched_gsi}")


def process_shard(
    src_label,
    dest_label,
    dest_endpoint,
    streams_client,
    ddb_client,
    table,
    stream_arn,
    shard_id,
    iterator,
):
    if not iterator:
        return None

    try:
        resp = streams_client.get_records(ShardIterator=iterator, Limit=100)
        records = resp.get("Records", [])

        removes = [r for r in records if r["eventName"] == "REMOVE"]
        others = [r for r in records if r["eventName"] in ("INSERT", "MODIFY")]

        if removes:
            try:
                apply_batch_removes(ddb_client, table, removes)
            except Exception as e:
                for r in removes:
                    check_lag(r, src_label, table)
                    write_to_dlq(src_label, dest_label, table, r, str(e))

        for record in others:
            check_lag(record, src_label, table)
            try:
                apply_record(ddb_client, table, dest_endpoint, record)
            except Exception as e:
                write_to_dlq(src_label, dest_label, table, record, str(e))

        next_iter = resp.get("NextShardIterator")

        # PERSISTENCE: Save the last sequence number of THIS batch
        if records:
            last_seq = records[-1]["dynamodb"]["SequenceNumber"]
            update_shard_sequence(src_label, table, shard_id, last_seq)

        return next_iter
    except streams_client.exceptions.ExpiredIteratorException:
        log.warning(f"Iterator expired for {shard_id[:12]}...")
        return streams_client.get_shard_iterator(
            StreamArn=stream_arn, ShardId=shard_id, ShardIteratorType="LATEST"
        )["ShardIterator"]
    except Exception as e:
        log.error(f"Error polling shard {shard_id[:12]}: {e}")
        return iterator


def coordinator_loop(region_clients, global_tables):
    if not global_tables:
        log.info("No global tables configured. Coordinator idling.")
        while not _shutdown.is_set():
            time.sleep(5)
        log.info("Coordinator stopped.")
        return

    load_shard_state()
    tracked_shards = {}
    last_refresh = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while not _shutdown.is_set():
            if time.monotonic() - last_refresh > SHARD_REFRESH_SEC:
                for table in global_tables:
                    for src in region_clients:
                        if src["table"] != table:
                            continue
                        known = {
                            k[3]
                            for k in tracked_shards.keys()
                            if k[0] == src["label"] and k[2] == table
                        }
                        new_iters = build_shard_iterators(
                            src["streams"],
                            src["stream_arn"],
                            src["label"],
                            table,
                            known,
                        )
                        for shard_id, iterator in new_iters.items():
                            for dest in region_clients:
                                if (
                                    src["label"] == dest["label"]
                                    or dest["table"] != table
                                ):
                                    continue
                                tracked_shards[
                                    (src["label"], dest["label"], table, shard_id)
                                ] = iterator
                last_refresh = time.monotonic()
                save_shard_state()

            futures = {}
            for key, iterator in tracked_shards.items():
                if not iterator:
                    continue
                src_label, dest_label, table, shard_id = key

                src_client = next(
                    c["streams"]
                    for c in region_clients
                    if c["label"] == src_label and c["table"] == table
                )
                src_arn = next(
                    c["stream_arn"]
                    for c in region_clients
                    if c["label"] == src_label and c["table"] == table
                )
                dest_client = next(
                    c["ddb"]
                    for c in region_clients
                    if c["label"] == dest_label and c["table"] == table
                )
                dest_endpoint = next(
                    c["endpoint"]
                    for c in region_clients
                    if c["label"] == dest_label and c["table"] == table
                )

                futures[
                    executor.submit(
                        process_shard,
                        src_label,
                        dest_label,
                        dest_endpoint,
                        src_client,
                        dest_client,
                        table,
                        src_arn,
                        shard_id,
                        iterator,
                    )
                ] = key

            for future in concurrent.futures.as_completed(futures):
                key = futures[future]
                try:
                    tracked_shards[key] = future.result()
                except Exception as e:
                    log.error(f"Task result error for {key}: {e}")

            save_shard_state()
            _shutdown.wait(timeout=POLL_INTERVAL)

    save_shard_state()
    log.info("Coordinator stopped.")


def main():
    if not REGIONS or not GLOBAL_TABLES:
        log.error("REGIONS and GLOBAL_TABLES must be set.")
        sys.exit(1)

    region_entries = parse_regions()
    global_tables = parse_tables()

    clients = []
    active_tables = set()
    for table in global_tables:
        for region, port in region_entries:
            endpoint = f"http://localhost:{port}"
            ddb = make_ddb_client(port)
            try:
                # Pre-warm schema cache; this will fail if table doesn't exist
                fetch_table_schema(ddb, table, endpoint)
                stream_arn = get_stream_arn(ddb, table)

                clients.append(
                    {
                        "label": f"{region}:{port}",
                        "endpoint": endpoint,
                        "ddb": ddb,
                        "streams": make_streams_client(port),
                        "table": table,
                        "stream_arn": stream_arn,
                    }
                )
                active_tables.add(table)
            except Exception as e:
                log.warning(f"Skipping table '{table}' in {region}:{port}: {e}")

    # Only pass tables that were successfully initialized
    filtered_global_tables = [t for t in global_tables if t in active_tables]

    log.info(
        f"Initialized {len(clients)} regional table clients for {len(filtered_global_tables)} tables. Starting Coordinator."
    )
    coordinator_loop(clients, filtered_global_tables)


if __name__ == "__main__":
    main()
