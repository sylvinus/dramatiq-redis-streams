import time

import dramatiq
import pytest

from dramatiq_redis_streams import StreamsBroker
from dramatiq_redis_streams.keys import dlq_stream_key, stream_key

from .conftest import make_message


# ---------------------------------------------------------------------------
# Basic read
# ---------------------------------------------------------------------------

class TestConsumerRead:
    def test_reads_enqueued_message(self, broker):
        msg = make_message(args=(1, 2), kwargs={"k": "v"})
        broker.enqueue(msg)

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)

        assert proxy is not None
        assert proxy.actor_name == "test-actor"
        assert proxy.args == (1, 2)
        assert proxy.kwargs == {"k": "v"}
        consumer.close()

    def test_preserves_message_id(self, broker):
        msg = make_message()
        broker.enqueue(msg)

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)

        assert proxy.message_id == msg.message_id
        consumer.close()

    def test_returns_none_on_timeout(self, broker):
        consumer = broker.consume("test-queue", timeout=100)
        result = next(consumer)
        assert result is None
        consumer.close()

    def test_prefetch_multiple(self, broker):
        for i in range(5):
            broker.enqueue(make_message(args=(i,)))

        consumer = broker.consume("test-queue", prefetch=5, timeout=1000)

        received = []
        for _ in range(5):
            proxy = next(consumer)
            if proxy is not None:
                received.append(proxy.args[0])

        assert len(received) == 5
        consumer.close()

    def test_has_redis_stream_id(self, broker):
        broker.enqueue(make_message())

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        assert hasattr(proxy, "_redis_stream_id")
        assert proxy._redis_stream_id is not None
        consumer.close()


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------

class TestConsumerAck:
    def test_removes_from_stream(self, broker, redis_client):
        broker.enqueue(make_message())

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.ack(proxy)

        assert redis_client.xlen(stream_key("test-queue")) == 0

    def test_clears_pending(self, broker, redis_client):
        broker.enqueue(make_message())

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.ack(proxy)

        groups = redis_client.xinfo_groups(stream_key("test-queue"))
        assert groups[0]["pending"] == 0


# ---------------------------------------------------------------------------
# nack
# ---------------------------------------------------------------------------

class TestConsumerNack:
    def test_moves_to_dlq(self, broker, redis_client):
        broker.enqueue(make_message())

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.nack(proxy)

        dlq_key = dlq_stream_key("test-queue")
        assert redis_client.xlen(dlq_key) == 1

    def test_removes_from_main_stream(self, broker, redis_client):
        broker.enqueue(make_message())

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.nack(proxy)

        assert redis_client.xlen(stream_key("test-queue")) == 0

    def test_dlq_contains_correct_message(self, broker, redis_client):
        msg = make_message(args=("bad-payload",))
        broker.enqueue(msg)

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.nack(proxy)

        entries = redis_client.xrange(dlq_stream_key("test-queue"))
        raw = entries[0][1].get(b"data") or entries[0][1].get("data")
        dead = dramatiq.Message.decode(raw)
        assert dead.args == ("bad-payload",)
        consumer.close()


# ---------------------------------------------------------------------------
# requeue
# ---------------------------------------------------------------------------

class TestConsumerRequeue:
    def test_readds_message_to_stream(self, broker, redis_client):
        broker.enqueue(make_message(args=(42,)))

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.requeue([proxy])

        # Old entry deleted, new entry added → length should be 1.
        assert redis_client.xlen(stream_key("test-queue")) == 1

    def test_requeued_message_consumable(self, broker):
        broker.enqueue(make_message(args=(42,)))

        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.requeue([proxy])

        proxy2 = next(consumer)
        assert proxy2 is not None
        assert proxy2.args == (42,)
        consumer.close()

    def test_requeue_multiple(self, broker, redis_client):
        for i in range(3):
            broker.enqueue(make_message(args=(i,)))

        consumer = broker.consume("test-queue", prefetch=3, timeout=1000)
        proxies = []
        for _ in range(3):
            p = next(consumer)
            if p is not None:
                proxies.append(p)

        consumer.requeue(proxies)
        assert redis_client.xlen(stream_key("test-queue")) == 3
        consumer.close()


# ---------------------------------------------------------------------------
# XAUTOCLAIM
# ---------------------------------------------------------------------------

class TestConsumerAutoclaim:
    @pytest.mark.timeout(10)
    def test_recovers_orphaned_messages(self, broker, redis_client):
        broker.enqueue(make_message(args=(99,)))

        # Consumer A reads but never acks — simulates a crash.
        consumer_a = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer_a)
        assert proxy is not None
        consumer_a._closed = True  # prevent any cleanup

        # Let the message become idle.
        time.sleep(1.5)

        # Consumer B with aggressive autoclaim settings.
        broker_b = StreamsBroker(client=redis_client, middleware=[])
        broker_b.broker_id = "consumer-b"
        consumer_b = broker_b.consume("test-queue", timeout=500)
        consumer_b.min_idle_time = 1000   # 1 s
        consumer_b.autoclaim_interval = 0  # run autoclaim every call

        proxy2 = next(consumer_b)
        assert proxy2 is not None
        assert proxy2.args == (99,)
        consumer_b.close()
