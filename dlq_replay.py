"""
Replays failed replication records from the Dead Letter Queue directory.

Each file in DLQ_DIR is a JSON record written by replicator.py when a
put_item or delete_item failed. This script reads those files, re-applies
the records to the correct destination instance, and removes successfully
replayed files.

Usage:
  # From host (docker exec into container):
  docker exec dynamoatlas python3 /app/dlq_replay.py

  # Dry run — show what would be replayed without applying:
  docker exec dynamoatlas python3 /app/dlq_replay.py --dry-run

  # Replay only a specific table:
  docker exec dynamoatlas python3 /app/dlq_replay.py --table Tenant

  # Replay only failures targeting a specific destination:
  docker exec dynamoatlas python3 /app/dlq_replay.py --dest eu-central-1:8000

  # Keep DLQ files even after successful replay (default: delete on success):
  docker exec dynamoatlas python3 /app/dlq_replay.py --keep
"""

import argparse
import boto3
import json
import logging
import os
import sys

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [dlq-replay] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DLQ_DIR = Path(os.environ.get("DLQ_DIR", "/tmp/dlq"))
REPL_SENTINEL_ATTR = "__dynamoatlas_repl__"

deserializer = TypeDeserializer()
serializer = TypeSerializer()


def make_ddb_client(port: str):
    return boto3.client(
        "dynamodb",
        endpoint_url=f"http://localhost:{port}",
        region_name="us-east-1",
        aws_access_key_id="fake",
        aws_secret_access_key="fake",
    )


def port_from_label(label: str) -> str:
    """Extract port from 'eu-central-1:8000' → '8000'"""
    return label.split(":")[-1]


def deserialize_item(raw: dict) -> dict:
    return {k: deserializer.deserialize(v) for k, v in raw.items()}


def serialize_item(item: dict) -> dict:
    return {k: serializer.serialize(v) for k, v in item.items()}


def get_key_schema(client, table_name: str) -> dict:
    resp = client.describe_table(TableName=table_name)
    schema = resp["Table"]["KeySchema"]
    pk = next(k["AttributeName"] for k in schema if k["KeyType"] == "HASH")
    sk = next((k["AttributeName"] for k in schema if k["KeyType"] == "RANGE"), None)
    return {"pk": pk, "sk": sk}


def replay_entry(entry: dict, dry_run: bool) -> bool:
    """
    Replay one DLQ record. Returns True if successful (or dry run), False on failure.
    """
    dest = entry["dest"]
    table_name = entry["table"]
    record = entry["record"]
    event = record.get("eventName")
    ddb_data = record.get("dynamodb", {})

    port = port_from_label(dest)
    client = make_ddb_client(port)

    log.info(f"Replaying {event} → '{table_name}' on {dest}")

    if dry_run:
        log.info(f"[dry-run] would apply {event} to '{table_name}' on {dest}")
        return True

    try:
        if event in ("INSERT", "MODIFY"):
            new_image = ddb_data.get("NewImage", {})
            if not new_image:
                log.warning(f"No NewImage in record — skipping")
                return True

            item = deserialize_item(new_image)

            # Use same two-phase sentinel write as replicator.py
            item_with_sentinel = {**item, REPL_SENTINEL_ATTR: True}
            client.put_item(
                TableName=table_name,
                Item=serialize_item(item_with_sentinel),
            )

            # Strip sentinel
            schema = get_key_schema(client, table_name)
            key = {
                schema["pk"]: serialize_item({schema["pk"]: item[schema["pk"]]})[
                    schema["pk"]
                ]
            }
            if schema["sk"] and schema["sk"] in item:
                key[schema["sk"]] = serialize_item({schema["sk"]: item[schema["sk"]]})[
                    schema["sk"]
                ]

            client.update_item(
                TableName=table_name,
                Key=key,
                UpdateExpression="REMOVE #s",
                ExpressionAttributeNames={"#s": REPL_SENTINEL_ATTR},
            )
            log.info(f"PUT replayed")

        elif event == "REMOVE":
            raw_keys = ddb_data.get("Keys", {})
            keys = deserialize_item(raw_keys)
            client.delete_item(
                TableName=table_name,
                Key=serialize_item(keys),
            )
            log.info(f"DELETE replayed")

        else:
            log.warning(f"Unknown event type '{event}' — skipping")

        return True

    except Exception as e:
        log.error(f"Replay failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Replay DynamoAtlas dead-lettered records"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be replayed without applying",
    )
    parser.add_argument(
        "--table", default=None, help="Only replay records for this table name"
    )
    parser.add_argument(
        "--dest",
        default=None,
        help="Only replay records targeting this destination (e.g. eu-central-1:8000)",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep DLQ files after successful replay (default: delete)",
    )
    parser.add_argument(
        "--dlq-dir", default=str(DLQ_DIR), help=f"DLQ directory (default: {DLQ_DIR})"
    )
    args = parser.parse_args()

    dlq_path = Path(args.dlq_dir)

    if not dlq_path.exists():
        log.info(f"DLQ directory '{dlq_path}' does not exist — nothing to replay.")
        sys.exit(0)

    files = sorted(dlq_path.glob("*.json"))
    if not files:
        log.info("No DLQ files found — nothing to replay.")
        sys.exit(0)

    log.info(f"Found {len(files)} DLQ file(s) in {dlq_path}")
    if args.dry_run:
        log.info("DRY RUN — no changes will be applied")

    success_count = 0
    failure_count = 0
    skipped_count = 0

    for filepath in files:
        try:
            entry = json.loads(filepath.read_text())
        except Exception as e:
            log.error(f"Could not parse {filepath.name}: {e}")
            failure_count += 1
            continue

        if args.table and entry.get("table") != args.table:
            log.debug(f"Skipping {filepath.name} (table filter)")
            skipped_count += 1
            continue

        if args.dest and entry.get("dest") != args.dest:
            log.debug(f"Skipping {filepath.name} (dest filter)")
            skipped_count += 1
            continue

        log.info(f"Processing: {filepath.name}")
        log.info(
            f"src={entry.get('src')}  dest={entry.get('dest')}  "
            f"table={entry.get('table')}  ts={entry.get('timestamp')}"
        )
        if entry.get("error"):
            log.info(f"original error: {entry['error']}")

        ok = replay_entry(entry, dry_run=args.dry_run)

        if ok:
            success_count += 1
            if not args.dry_run and not args.keep:
                filepath.unlink()
                log.info(f"Deleted {filepath.name}")
        else:
            failure_count += 1

    log.info("─" * 50)
    log.info(f"Replay complete:")
    log.info(f"Success:  {success_count}")
    log.info(f"Failed:   {failure_count}")
    log.info(f"Skipped:  {skipped_count}")

    if failure_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
