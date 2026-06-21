"""WSGI integration tests for the dashboard application."""

import json
from io import BytesIO

import dramatiq

from dramatiq_redis_streams.dashboard.app import _MAX_PENDING_DETAIL, DashboardApp
from dramatiq_redis_streams.keys import dlq_stream_key, queues_key, stream_key


def make_message(queue="test-queue", actor="test-actor", args=(), kwargs=None):
    return dramatiq.Message(
        queue_name=queue,
        actor_name=actor,
        args=args,
        kwargs=kwargs or {},
        options={},
    )


def _request(app, method, path, body=None, query=""):
    """Simulate a WSGI request and return (status_code, headers, body_bytes)."""
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query,
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

    def test_discovers_queues_from_registry(self, broker, redis_client):
        """A queue registered by a worker (in another process) is listed even
        though this dashboard's broker never declared it."""
        redis_client.sadd(queues_key(broker.namespace), "worker-only")
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "GET", "/api/overview")
        assert status == 200
        assert "worker-only" in [q["name"] for q in json.loads(body)["queues"]]


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

    def test_requeue_all_dlq(self, broker, redis_client):
        broker.declare_queue("work")
        dk = dlq_stream_key("work", broker.namespace)
        sk = stream_key("work", broker.namespace)
        for _ in range(3):
            redis_client.xadd(dk, {"data": make_message(queue="work").encode()})
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", "/api/queues/work/dlq/requeue-all")
        assert status == 200
        data = json.loads(body)
        assert data["requeued"] == 3
        assert redis_client.xlen(dk) == 0
        assert redis_client.xlen(sk) == 3


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


class TestRemoveAPI:
    def test_remove_queue(self, broker, redis_client):
        redis_client.sadd(queues_key(broker.namespace), "work")
        redis_client.xadd(stream_key("work", broker.namespace), {"data": b"x"})
        app = DashboardApp(broker)
        status, _headers, body = _request(app, "POST", "/api/queues/work/remove")
        assert status == 200
        assert json.loads(body)["ok"] is True
        assert not redis_client.exists(stream_key("work", broker.namespace))
        members = redis_client.smembers(queues_key(broker.namespace))
        assert b"work" not in members and "work" not in members


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

    def test_pending_param_is_clamped(self, broker):
        """An over-large ?pending value is capped at the hard ceiling."""
        n = _MAX_PENDING_DETAIL + 50
        broker.declare_queue("work")
        for i in range(n):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=n, timeout=1000)
        msgs = [next(consumer) for _ in range(n)]

        app = DashboardApp(broker)
        status, _headers, body = _request(
            app, "GET", "/api/workers", query="pending=100000",
        )
        assert status == 200
        worker = next(w for w in json.loads(body) if w["name"].startswith("worker-"))
        assert worker["total_pending"] == n                          # aggregate stays exact
        assert len(worker["pending_messages"]) == _MAX_PENDING_DETAIL  # detail hard-capped

        for m in msgs:
            consumer.ack(m)
        consumer.close()

    def test_bad_pending_param_falls_back(self, broker):
        """A non-numeric ?pending value falls back to the default, not a 500."""
        app = DashboardApp(broker)
        status, _headers, body = _request(
            app, "GET", "/api/workers", query="pending=abc",
        )
        assert status == 200
        assert json.loads(body) == []

    def test_worker_pending_pagination_route(self, broker):
        """GET /api/workers/<name>/pending pages through the worker's PEL."""
        broker.declare_queue("work")
        for i in range(5):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=5, timeout=1000)
        msgs = [next(consumer) for _ in range(5)]
        app = DashboardApp(broker)

        # Discover the worker name from the workers listing.
        _s, _h, body = _request(app, "GET", "/api/workers")
        wname = next(w["name"] for w in json.loads(body) if w["name"].startswith("worker-"))

        seen, cursor = [], None
        while True:
            query = "count=2" + (f"&after={cursor}" if cursor else "")
            status, headers, body = _request(
                app, "GET", f"/api/workers/{wname}/pending", query=query,
            )
            assert status == 200
            assert headers["Content-Type"] == "application/json"
            data = json.loads(body)
            seen.extend(m["id"] for m in data["messages"])
            cursor = data["next_cursor"]
            if cursor is None:
                break

        assert len(seen) == 5
        assert len(set(seen)) == 5

        for m in msgs:
            consumer.ack(m)
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
