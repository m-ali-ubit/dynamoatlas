from replicator import (
    deserialize_item,
    serialize_item,
    load_shard_state,
    save_shard_state,
    update_shard_sequence,
    parse_regions,
    parse_tables,
    fetch_table_schema,
    apply_record,
    REPL_SENTINEL_ATTR,
)
from unittest.mock import MagicMock, patch


def test_serialization_roundtrip():
    """Test that DynamoDB types are correctly serialized and deserialized."""
    raw = {
        "id": {"S": "123"},
        "count": {"N": "42"},
        "tags": {"L": [{"S": "a"}, {"S": "b"}]},
        "metadata": {"M": {"active": {"BOOL": True}}},
    }
    item = deserialize_item(raw)
    assert item["id"] == "123"
    assert item["count"] == 42
    assert item["tags"] == ["a", "b"]
    assert item["metadata"]["active"] is True
    reserialized = serialize_item(item)
    assert reserialized == raw


def test_persistence(tmp_path):
    """Test that shard state can be saved and loaded from disk."""
    test_file = tmp_path / "shard_state.json"

    import replicator

    original_state_file = replicator.STATE_FILE
    replicator.STATE_FILE = test_file

    try:
        update_shard_sequence("region:8000", "Table1", "shardId-001", "SEQ-123")
        update_shard_sequence("region:8000", "Table1", "shardId-002", "SEQ-456")
        save_shard_state()
        assert test_file.exists()
        replicator._state_cache = {}
        load_shard_state()
        assert replicator._state_cache["region:8000:Table1:shardId-001"] == "SEQ-123"
        assert replicator._state_cache["region:8000:Table1:shardId-002"] == "SEQ-456"
    finally:
        replicator.STATE_FILE = original_state_file


def test_parsing_helpers():
    """Test environment variable parsing logic."""
    regions = "eu-central-1:8000, us-east-1:8001 "
    with patch.dict("os.environ", {"REGIONS": regions}):
        with patch("replicator.REGIONS", regions):
            parsed = parse_regions()
            assert parsed == [["eu-central-1", "8000"], ["us-east-1", "8001"]]

    tables = "Table1, Table2 , Table3"
    with patch.dict("os.environ", {"GLOBAL_TABLES": tables}):
        with patch("replicator.GLOBAL_TABLES", tables):
            parsed = parse_tables()
            assert parsed == ["Table1", "Table2", "Table3"]

    with patch.dict("os.environ", {"GLOBAL_TABLES": ""}):
        with patch("replicator.GLOBAL_TABLES", ""):
            assert parse_tables() == []


def test_schema_caching():
    """Test that fetch_table_schema uses internal cache."""
    mock_client = MagicMock()
    mock_client.describe_table.return_value = {
        "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
    }

    with patch("replicator._table_schema_cache", {}):
        schema1 = fetch_table_schema(mock_client, "CacheTable", "http://localhost:8000")
        assert schema1["pk"] == "pk"
        assert "pk" in schema1["key_attrs"]
        assert mock_client.describe_table.call_count == 1

        schema2 = fetch_table_schema(mock_client, "CacheTable", "http://localhost:8000")
        assert schema2 == schema1
        assert mock_client.describe_table.call_count == 1


def test_apply_record_sentinel():
    """Test that apply_record handles the replication sentinel correctly."""
    mock_client = MagicMock()
    mock_client.describe_table.return_value = {
        "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
    }

    with patch("replicator._table_schema_cache", {}):
        record = {
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {"id": {"S": "item1"}, "val": {"N": "100"}},
                "Keys": {"id": {"S": "item1"}},
            },
        }

        apply_record(mock_client, "SentinelTable", "http://localhost:8000", record)

        args, kwargs = mock_client.put_item.call_args
        assert kwargs["Item"]["id"]["S"] == "item1"
        assert kwargs["Item"][REPL_SENTINEL_ATTR]["BOOL"] is True

    args, kwargs = mock_client.update_item.call_args
    assert kwargs["Key"] == {"id": {"S": "item1"}}
    assert "REMOVE #s" in kwargs["UpdateExpression"]
    assert kwargs["ExpressionAttributeNames"]["#s"] == REPL_SENTINEL_ATTR
