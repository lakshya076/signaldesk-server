"""Rate limit tests. Need a Redis on localhost:6379; run from backend/: pytest test_rate_limit.py"""
import os

os.environ.setdefault("URL", "http://localhost:54321")
os.environ.setdefault("SERVICE_KEY", "test")
os.environ.setdefault("ANON_KEY", "test")

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import main


@pytest.fixture
def client():
    for key in main.redis_client.scan_iter("ratelimit:*"):
        main.redis_client.delete(key)
    with TestClient(main.app) as c:
        yield c


def test_http_limit_returns_429_with_retry_after(client, monkeypatch):
    monkeypatch.setitem(main.RATE_LIMITS, "/health", 3)
    assert [client.get("/health").status_code for _ in range(4)] == [200, 200, 200, 429]
    r = client.get("/health")
    assert 0 < int(r.headers["Retry-After"]) <= 60
    assert client.get("/").status_code == 200  # other endpoints use their own bucket


def test_websocket_connection_limit(client, monkeypatch):
    monkeypatch.setitem(main.RATE_LIMITS, "/ws/prices", 1)
    with client.websocket_connect("/ws/prices"):
        pass
    with pytest.raises(WebSocketDisconnect) as e:
        with client.websocket_connect("/ws/prices"):
            pass
    assert e.value.code == 1008


def test_websocket_message_limit(client, monkeypatch):
    monkeypatch.setattr(main, "WS_MESSAGES_PER_MIN", 2)
    with client.websocket_connect("/ws/prices") as ws:
        for _ in range(2):
            ws.send_json({"action": "subscribe", "symbols": ["HDFCBANK"]})
            assert ws.receive_json()["status"] == "success"
        ws.send_json({"action": "subscribe", "symbols": ["HDFCBANK"]})
        with pytest.raises(WebSocketDisconnect) as e:
            ws.receive_json()
        assert e.value.code == 1008
