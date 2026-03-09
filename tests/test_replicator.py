from replicator import (
    deserialize_item,
    serialize_item,
    load_shard_state,
    save_shard_state,
    update_shard_sequence,
)


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
    # Override STATE_FILE to a temp path for testing
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
