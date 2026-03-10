import pytest
import os
from unittest.mock import MagicMock, patch

os.environ["REGIONS"] = "us-east-1:8000"
os.environ["GLOBAL_TABLES"] = "Table1"
from init import HealthHandler, ensure_stream_enabled


class MockRequest:
    def __init__(self, path):
        self.path = path


class MockBaseHandler:
    def __init__(self):
        self.wfile = MagicMock()
        self.headers = {}

    def send_response(self, code):
        self.response_code = code

    def send_header(self, keyword, value):
        self.headers[keyword] = value

    def end_headers(self):
        pass


def test_health_endpoint_starting():
    """Test health endpoint returns 503 while starting."""
    mock_wfile = MagicMock()
    handler = MagicMock()
    handler.wfile = mock_wfile
    handler.path = "/health"

    with patch("init._health_lock"):
        with patch("init._health_state", {"status": "starting", "checks": {}}):
            HealthHandler.do_GET(handler)
            handler.send_response.assert_called_with(503)


def test_health_endpoint_ready():
    """Test health endpoint returns 200 when ready."""
    mock_wfile = MagicMock()
    handler = MagicMock()
    handler.wfile = mock_wfile
    handler.path = "/health"

    with patch("init._health_lock"):
        with patch("init._health_state", {"status": "ready", "checks": {}}):
            HealthHandler.do_GET(handler)
            handler.send_response.assert_called_with(200)


def test_ensure_stream_enabled_already_active():
    """Test that it skips if stream is already enabled."""
    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "StreamSpecification": {
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            }
        }
    }

    ensure_stream_enabled(client, "us-east-1", "MyTable")
    client.update_table.assert_not_called()


def test_ensure_stream_enabled_activates():
    """Test that it calls update_table if stream is missing."""
    client = MagicMock()
    client.describe_table.return_value = {"Table": {}}

    ensure_stream_enabled(client, "us-east-1", "MyTable")
    client.update_table.assert_called_once()
    args, kwargs = client.update_table.call_args
    assert kwargs["StreamSpecification"]["StreamEnabled"] is True
    assert kwargs["StreamSpecification"]["StreamViewType"] == "NEW_AND_OLD_IMAGES"
