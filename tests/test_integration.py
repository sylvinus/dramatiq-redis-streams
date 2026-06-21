import threading
import time

import dramatiq
import pytest
from dramatiq import Worker

from dramatiq_redis_streams import StreamsBroker
from dramatiq_redis_streams.keys import dlq_stream_key

from .conftest import make_message, wait_for


# ---------------------------------------------------------------------------
# Full-lifecycle tests
# ---------------------------------------------------------------------------

class TestFullWorkflow:
    def test_enqueue_consume_ack(self, broker):
        msg = make_message(args=(1, 2, 3), kwargs={"x": "y"})
        broker.enqueue(msg)

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)

        assert proxy is not None
        assert proxy.actor_name == "test-actor"
        assert proxy.args == (1, 2, 3)
        assert proxy.kwargs == {"x": "y"}
        assert proxy.message_id == msg.message_id

        consumer.ack(proxy)

        # Nothing left.
        proxy2 = next(consumer)
        assert proxy2 is None
        consumer.close()

    def test_nack_sends_to_dlq(self, broker, redis_client):
        broker.enqueue(make_message(args=("bad",)))

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.nack(proxy)

        entries = redis_client.xrange(dlq_stream_key("test-queue"))
        assert len(entries) == 1

        dead = dramatiq.Message.decode(
            entries[0][1].get(b"data") or entries[0][1].get("data")
        )
        assert dead.args == ("bad",)
        consumer.close()

    def test_requeue_allows_reprocessing(self, broker):
        broker.enqueue(make_message(args=("retry-me",)))

        consumer = broker.consume("test-queue", timeout=1000)

        proxy1 = next(consumer)
        assert proxy1 is not None
        consumer.requeue([proxy1])

        proxy2 = next(consumer)
        assert proxy2 is not None
        assert proxy2.args == ("retry-me",)
        consumer.ack(proxy2)
        consumer.close()


# ---------------------------------------------------------------------------
# Delayed messages (end-to-end, uses the broker's built-in scheduler)
# ---------------------------------------------------------------------------

class TestDelayedE2E:
    @pytest.mark.timeout(10)
    def test_delayed_message_delivered(self, broker):
        broker.enqueue(make_message(args=("scheduled",)), delay=200)

        consumer = broker.consume("test-queue", timeout=2000)

        proxy = None
        deadline = time.time() + 5
        while time.time() < deadline:
            proxy = next(consumer)
            if proxy is not None:
                break

        assert proxy is not None
        assert proxy.args == ("scheduled",)
        consumer.ack(proxy)
        consumer.close()


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------

class TestOrdering:
    def test_fifo_order(self, broker):
        for i in range(10):
            broker.enqueue(make_message(args=(i,)))

        consumer = broker.consume("test-queue", prefetch=1, timeout=1000)

        received = []
        for _ in range(10):
            proxy = next(consumer)
            assert proxy is not None
            received.append(proxy.args[0])
            consumer.ack(proxy)

        assert received == list(range(10))
        consumer.close()


# ---------------------------------------------------------------------------
# Multi-queue
# ---------------------------------------------------------------------------

class TestMultiQueue:
    def test_messages_route_to_correct_queue(self, broker):
        broker.enqueue(make_message(queue="queue-a", actor="actor-a", args=("a",)))
        broker.enqueue(make_message(queue="queue-b", actor="actor-b", args=("b",)))

        ca = broker.consume("queue-a", timeout=1000)
        cb = broker.consume("queue-b", timeout=1000)

        pa = next(ca)
        pb = next(cb)

        assert pa.actor_name == "actor-a"
        assert pb.actor_name == "actor-b"
        ca.close()
        cb.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

class TestRealWorker:
    """End-to-end through dramatiq's actual Worker machinery (not just
    broker.consume) — the seam where prefetch/ack/reclaim bugs hide."""

    @pytest.mark.timeout(20)
    def test_worker_processes_all_messages(self, broker):
        dramatiq.set_broker(broker)
        processed = []
        lock = threading.Lock()

        @dramatiq.actor(queue_name="rw")
        def record(x):
            with lock:
                processed.append(x)

        for i in range(30):
            record.send(i)

        worker = Worker(broker, worker_threads=2)
        worker.start()
        try:
            ok = wait_for(lambda: len(processed) >= 30, timeout=15)
            assert ok, f"processed only {len(processed)}/30"
            # Exactly-once under the happy path (no duplicates from over-reserve
            # or premature reclaim).
            assert sorted(processed) == list(range(30))
        finally:
            worker.stop()


class TestConcurrency:
    @pytest.mark.timeout(15)
    def test_two_consumers_share_work(self, broker, redis_client):
        for i in range(20):
            broker.enqueue(make_message(args=(i,)))

        results = []
        lock = threading.Lock()

        def consume_all(broker_inst):
            consumer = broker_inst.consume("test-queue", timeout=500)
            while True:
                proxy = next(consumer)
                if proxy is None:
                    break
                with lock:
                    results.append(proxy.args[0])
                consumer.ack(proxy)
            consumer.close()

        broker2 = StreamsBroker(client=redis_client, middleware=[])
        broker2.broker_id = "consumer-2"

        t1 = threading.Thread(target=consume_all, args=(broker,))
        t2 = threading.Thread(target=consume_all, args=(broker2,))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert sorted(results) == list(range(20))
