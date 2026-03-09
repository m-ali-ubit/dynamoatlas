import pytest
from dlq_replay import replay_entry


def test_replay_insert(ddb_client, tmp_path):
    """Test that an INSERT record from DLQ is successfully replayed."""
    table_name = "TestTable"
    ddb_client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    entry = {
        "dest": "region:8000",
        "table": table_name,
        "record": {
            "eventName": "INSERT",
            "dynamodb": {"NewImage": {"pk": {"S": "item1"}, "data": {"S": "payload"}}},
        },
    }
    import dlq_replay
    original_make_client = dlq_replay.make_ddb_client
    dlq_replay.make_ddb_client = lambda port: ddb_client
    try:
        success = replay_entry(entry, dry_run=False)
        assert success is True
        resp = ddb_client.get_item(TableName=table_name, Key={"pk": {"S": "item1"}})
        assert "Item" in resp
        assert resp["Item"]["data"]["S"] == "payload"
        # Verify sentinel was removed (replay_entry does put then update)
        assert "__dynamoatlas_repl__" not in resp["Item"]
    finally:
        dlq_replay.make_ddb_client = original_make_client


def test_replay_remove(ddb_client):
    """Test that a REMOVE record from DLQ is successfully replayed."""
    table_name = "TestTable"
    ddb_client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    ddb_client.put_item(TableName=table_name, Item={"pk": {"S": "item_to_delete"}})
    entry = {
        "dest": "region:8000",
        "table": table_name,
        "record": {
            "eventName": "REMOVE",
            "dynamodb": {"Keys": {"pk": {"S": "item_to_delete"}}},
        },
    }
    import dlq_replay
    original_make_client = dlq_replay.make_ddb_client
    dlq_replay.make_ddb_client = lambda port: ddb_client
    try:
        success = replay_entry(entry, dry_run=False)
        assert success is True
        resp = ddb_client.get_item(
            TableName=table_name, Key={"pk": {"S": "item_to_delete"}}
        )
        assert "Item" not in resp
    finally:
        dlq_replay.make_ddb_client = original_make_client
