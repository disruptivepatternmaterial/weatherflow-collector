# test_geo_strike.py

"""
Tests for the geo_strike websocket handler.

The strike time arrives as `time`, but the publish step reads `timestamp`. The
handler stored the time under `time`, so every geo-strike raised KeyError before
it reached storage.

Run from the repository root:

    python3 -m pytest tests/ -v
"""

import asyncio
import os
import sys
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# config.py requires the API token at import time; no network calls are made
os.environ.setdefault("WEATHERFLOW_COLLECTOR_API_TOKEN", "test-token")

from handlers.websocket import WebSocketHandler  # noqa: E402


def test_geo_strike_publishes_its_time_as_the_timestamp():
    event_manager = mock.Mock()
    event_manager.publish = mock.AsyncMock()
    handler = WebSocketHandler(event_manager)
    full_data = {
        "data": {
            "device_id": 42,
            "time": 1760000000,
            "lat": 50.1,
            "lon": 14.4,
            "mag": 3.0,
            "strike_type": 1,
        },
        "station_info": {"station_name": "Test"},
        "device_info": {},
    }

    asyncio.run(handler.handle_geo_strike(full_data))

    storage = [
        call.args[1]
        for call in event_manager.publish.await_args_list
        if call.args[0] == "influxdb_storage_event" and call.args[1]["measurement"] == "weatherflow_geo_strike"
    ]
    assert len(storage) == 1
    assert storage[0]["timestamp"] == 1760000000
    assert storage[0]["fields"]["timestamp"] == 1760000000
    assert "time" not in storage[0]["fields"]
