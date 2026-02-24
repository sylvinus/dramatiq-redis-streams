"""Tests for the dashboard data-layer functions."""

import dramatiq
import pytest

from dramatiq_redis_streams.dashboard import api
from dramatiq_redis_streams.keys import delayed_key, dlq_stream_key, stream_key


def make_message(queue="test-queue", actor="test-actor", args=(), kwargs=None):
    return dramatiq.Message(
        queue_name=queue,
        actor_name=actor,
        args=args,
        kwargs=kwargs or {},
        options={},
    )


class TestGetOverview:
    def test_empty_broker(self, broker, redis_client):
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        assert result == {"queues": [], "delayed_count": 0}

    def test_with_declared_queues(self, broker, redis_client):
        broker.declare_queue("orders")
        broker.declare_queue("emails")
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        assert len(result["queues"]) == 2
        names = [q["name"] for q in result["queues"]]
        assert "orders" in names
        assert "emails" in names

    def test_stream_length(self, broker, redis_client):
        broker.declare_queue("work")
        msg = make_message(queue="work")
        broker.enqueue(msg)
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        q = next(q for q in result["queues"] if q["name"] == "work")
        assert q["stream_length"] == 1

    def test_dlq_count(self, broker, redis_client):
        broker.declare_queue("work")
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work")
        redis_client.xadd(dk, {"data": msg.encode()})
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        q = next(q for q in result["queues"] if q["name"] == "work")
        assert q["dlq_length"] == 1

    def test_delayed_count(self, broker, redis_client):
        broker.declare_queue("work")
        msg = make_message(queue="work")
        broker.enqueue(msg, delay=60000)
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        assert result["delayed_count"] == 1

    def test_discovers_undeclared_queues(self, broker, redis_client):
        """Queues created by other processes are discovered via SCAN."""
        sk = stream_key("other-process-queue", broker.namespace)
        redis_client.xadd(sk, {"data": b"test"})
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        names = [q["name"] for q in result["queues"]]
        assert "other-process-queue" in names


class TestGetQueueMessages:
    def test_empty_queue(self, broker, redis_client):
        result = api.get_queue_messages(redis_client, broker.namespace, "empty")
        assert result == []

    def test_returns_messages(self, broker, redis_client):
        broker.declare_queue("work")
        msg = make_message(queue="work", actor="do_stuff", args=(1, 2))
        broker.enqueue(msg)
        result = api.get_queue_messages(redis_client, broker.namespace, "work")
        assert len(result) == 1
        assert result[0]["actor"] == "do_stuff"
        assert result[0]["args"] == [1, 2]
        assert result[0]["id"]  # has a stream ID

    def test_count_limit(self, broker, redis_client):
        broker.declare_queue("work")
        for i in range(10):
            broker.enqueue(make_message(queue="work", actor=f"actor_{i}"))
        result = api.get_queue_messages(redis_client, broker.namespace, "work", count=3)
        assert len(result) == 3

    def test_corrupted_entry(self, broker, redis_client):
        """Corrupted stream entries decode gracefully."""
        sk = stream_key("corrupt", broker.namespace)
        redis_client.xadd(sk, {"data": b"not-valid-json"})
        result = api.get_queue_messages(redis_client, broker.namespace, "corrupt")
        assert len(result) == 1
        assert result[0]["actor"] == "<decode error>"


class TestGetDlqMessages:
    def test_empty_dlq(self, broker, redis_client):
        result = api.get_dlq_messages(redis_client, broker.namespace, "work")
        assert result == []

    def test_returns_dlq_messages(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work", actor="failed_task")
        redis_client.xadd(dk, {"data": msg.encode()})
        result = api.get_dlq_messages(redis_client, broker.namespace, "work")
        assert len(result) == 1
        assert result[0]["actor"] == "failed_task"


class TestGetDelayedMessages:
    def test_empty(self, broker, redis_client):
        result = api.get_delayed_messages(redis_client, broker.namespace)
        assert result == []

    def test_returns_delayed(self, broker, redis_client):
        broker.declare_queue("work")
        msg = make_message(queue="work", actor="later_task")
        broker.enqueue(msg, delay=60000)
        result = api.get_delayed_messages(redis_client, broker.namespace)
        assert len(result) == 1
        assert result[0]["actor"] == "later_task"
        assert result[0]["eta_ms"] > 0


class TestDeleteDlqMessage:
    def test_delete_existing(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work")
        stream_id = redis_client.xadd(dk, {"data": msg.encode()})
        sid = stream_id if isinstance(stream_id, str) else stream_id.decode()
        assert api.delete_dlq_message(redis_client, broker.namespace, "work", sid) is True
        assert redis_client.xlen(dk) == 0

    def test_delete_nonexistent(self, broker, redis_client):
        result = api.delete_dlq_message(redis_client, broker.namespace, "work", "0-0")
        assert result is False


class TestRequeueDlqMessage:
    def test_requeue_moves_to_main_stream(self, broker, redis_client):
        broker.declare_queue("work")
        dk = dlq_stream_key("work", broker.namespace)
        sk = stream_key("work", broker.namespace)
        msg = make_message(queue="work", actor="retry_me")
        stream_id = redis_client.xadd(dk, {"data": msg.encode()})
        sid = stream_id if isinstance(stream_id, str) else stream_id.decode()

        assert api.requeue_dlq_message(redis_client, broker.namespace, "work", sid) is True
        # DLQ should be empty
        assert redis_client.xlen(dk) == 0
        # Main stream should have the message
        assert redis_client.xlen(sk) >= 1

    def test_requeue_nonexistent(self, broker, redis_client):
        result = api.requeue_dlq_message(redis_client, broker.namespace, "work", "0-0")
        assert result is False


class TestPurgeDlq:
    def test_purge(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        for _ in range(5):
            redis_client.xadd(dk, {"data": make_message(queue="work").encode()})
        count = api.purge_dlq(redis_client, broker.namespace, "work")
        assert count == 5
        assert redis_client.xlen(dk) == 0

    def test_purge_empty(self, broker, redis_client):
        count = api.purge_dlq(redis_client, broker.namespace, "work")
        assert count == 0


class TestFlushQueue:
    def test_flush(self, broker, redis_client):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        broker.enqueue(make_message(queue="work"))
        sk = stream_key("work", broker.namespace)
        assert redis_client.xlen(sk) == 2
        api.flush_queue(redis_client, broker.namespace, "work")
        assert redis_client.xlen(sk) == 0


class TestGetWorkers:
    def test_no_workers(self, broker, redis_client):
        """No consumers registered yet → empty list."""
        result = api.get_workers(redis_client, broker.namespace, broker.get_declared_queues())
        assert result == []

    def test_consumer_appears_after_read(self, broker, redis_client):
        """A consumer that has read a message shows up in the workers list."""
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work", actor="do_stuff"))
        consumer = broker.consume("work", prefetch=1, timeout=1000)
        msg = next(consumer)
        assert msg is not None

        result = api.get_workers(redis_client, broker.namespace, broker.get_declared_queues())
        assert len(result) >= 1
        worker = next(w for w in result if w["name"].startswith("worker-"))
        assert worker["status"] == "active"
        assert worker["total_pending"] >= 1
        assert "work" in worker["queues"]
        # Should have pending message details
        assert len(worker["pending_messages"]) >= 1
        assert worker["pending_messages"][0]["actor"] == "do_stuff"
        assert worker["pending_messages"][0]["queue"] == "work"
        assert worker["pending_messages"][0]["deliveries"] >= 1

        consumer.ack(msg)
        consumer.close()

    def test_worker_across_multiple_queues(self, broker, redis_client):
        """A worker consuming from two queues is aggregated into one entry."""
        broker.declare_queue("q1")
        broker.declare_queue("q2")
        broker.enqueue(make_message(queue="q1", actor="a1"))
        broker.enqueue(make_message(queue="q2", actor="a2"))

        c1 = broker.consume("q1", prefetch=1, timeout=1000)
        c2 = broker.consume("q2", prefetch=1, timeout=1000)
        m1 = next(c1)
        m2 = next(c2)

        result = api.get_workers(redis_client, broker.namespace, broker.get_declared_queues())
        # Both consumers share the same broker_id → same worker name
        worker = next(w for w in result if w["name"].startswith("worker-"))
        assert "q1" in worker["queues"]
        assert "q2" in worker["queues"]
        assert worker["total_pending"] == 2

        c1.ack(m1)
        c2.ack(m2)
        c1.close()
        c2.close()

    def test_no_pending_after_ack(self, broker, redis_client):
        """After acking, pending count drops to zero."""
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        consumer = broker.consume("work", prefetch=1, timeout=1000)
        msg = next(consumer)
        consumer.ack(msg)

        result = api.get_workers(redis_client, broker.namespace, broker.get_declared_queues())
        if result:
            worker = result[0]
            assert worker["total_pending"] == 0
            assert worker["pending_messages"] == []

        consumer.close()
