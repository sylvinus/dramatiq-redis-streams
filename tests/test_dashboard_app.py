"""WSGI integration tests for the dashboard application."""

import json
from io import BytesIO

import dramatiq

from dramatiq_redis_streams.dashboard.app import DashboardApp
from dramatiq_redis_streams.keys import dlq_stream_key


def make_message(queue="test-queue", actor="test-actor", args=(), kwargs=None):
    return dramatiq.Message(
        queue_name=queue,
        actor_name=actor,
        args=args,
        kwargs=kwargs or {},
        options={},
    )


def _request(app, method, path, body=None):
    """Simulate a WSGI request and return (status_code, headers, body_bytes)."""
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "SERVER_NAME": "localhost",
        "SERVER_PORT": "8080",
        "HTTP_HOST": "localhost:8080",
        "wsgi.input": BytesIO(body or b""),
        "wsgi.errors": BytesIO(),
    }

    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    body_parts = app(environ, start_response)
    response_body = b"".join(body_parts)
    status_code = int(captured["status"].split(" ", 1)[0])
    return status_code, dict(captured["headers"]), response_body


class TestIndexPage:
    def test_get_returns_html(self, broker):
        app = DashboardApp(broker)
        status, headers, body = _request(app, "GET", "/")
        assert status == 200
        assert "text/html" in headers["Content-Type"]
        assert b"Dramatiq Streams" in body

    def test_post_index_returns_405(self, broker):
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", "/")
        assert status == 405


class TestOverviewAPI:
    def test_returns_json(self, broker):
        app = DashboardApp(broker)
        status, headers, body = _request(app, "GET", "/api/overview")
        assert status == 200
        assert headers["Content-Type"] == "application/json"
        data = json.loads(body)
        assert "queues" in data
        assert "delayed_count" in data

    def test_includes_declared_queues(self, broker):
        broker.declare_queue("tasks")
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/overview")
        data = json.loads(body)
        names = [q["name"] for q in data["queues"]]
        assert "tasks" in names


class TestQueueMessagesAPI:
    def test_get_messages(self, broker):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work", actor="do_work"))
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/queues/work/messages")
        assert status == 200
        data = json.loads(body)
        assert len(data) == 1
        assert data[0]["actor"] == "do_work"


class TestDlqAPI:
    def test_get_dlq_messages(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work", actor="failed")
        redis_client.xadd(dk, {"data": msg.encode()})
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/queues/work/dlq")
        assert status == 200
        data = json.loads(body)
        assert len(data) == 1
        assert data[0]["actor"] == "failed"

    def test_requeue_dlq_message(self, broker, redis_client):
        broker.declare_queue("work")
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work", actor="retry")
        stream_id = redis_client.xadd(dk, {"data": msg.encode()})
        sid = stream_id if isinstance(stream_id, str) else stream_id.decode()
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", f"/api/queues/work/dlq/{sid}/requeue")
        assert status == 200
        data = json.loads(body)
        assert data["ok"] is True
        assert redis_client.xlen(dk) == 0

    def test_delete_dlq_message(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work")
        stream_id = redis_client.xadd(dk, {"data": msg.encode()})
        sid = stream_id if isinstance(stream_id, str) else stream_id.decode()
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", f"/api/queues/work/dlq/{sid}/delete")
        assert status == 200
        data = json.loads(body)
        assert data["ok"] is True

    def test_purge_dlq(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        for _ in range(3):
            redis_client.xadd(dk, {"data": make_message(queue="work").encode()})
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", "/api/queues/work/dlq/purge")
        assert status == 200
        data = json.loads(body)
        assert data["purged"] == 3


class TestDelayedAPI:
    def test_get_delayed(self, broker):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work", actor="later"), delay=60000)
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/delayed")
        assert status == 200
        data = json.loads(body)
        assert len(data) == 1
        assert data[0]["actor"] == "later"


class TestFlushAPI:
    def test_flush_queue(self, broker, redis_client):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", "/api/queues/work/flush")
        assert status == 200
        data = json.loads(body)
        assert data["ok"] is True


class TestWorkersAPI:
    def test_returns_empty_list(self, broker):
        app = DashboardApp(broker)
        status, headers, body = _request(app, "GET", "/api/workers")
        assert status == 200
        assert headers["Content-Type"] == "application/json"
        data = json.loads(body)
        assert data == []

    def test_returns_worker_after_consume(self, broker):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work", actor="task"))
        consumer = broker.consume("work", prefetch=1, timeout=1000)
        msg = next(consumer)

        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/workers")
        assert status == 200
        data = json.loads(body)
        assert len(data) >= 1
        assert data[0]["total_pending"] >= 1
        assert "work" in data[0]["queues"]

        consumer.ack(msg)
        consumer.close()


class TestRouting:
    def test_404_for_unknown_route(self, broker):
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/nonexistent")
        assert status == 404

    def test_405_for_wrong_method(self, broker):
        app = DashboardApp(broker)
        status, _headers, _body = _request(app, "POST", "/api/overview")
        assert status == 405

    def test_prefix_stripping(self, broker):
        app = DashboardApp(broker, prefix="/admin/dramatiq")
        status, headers, body = _request(app, "GET", "/admin/dramatiq/")
        assert status == 200
        assert b"Dramatiq Streams" in body

    def test_prefix_api(self, broker):
        app = DashboardApp(broker, prefix="/admin/dramatiq")
        status, _headers, body = _request(app, "GET", "/admin/dramatiq/api/overview")
        assert status == 200
        data = json.loads(body)
        assert "queues" in data
