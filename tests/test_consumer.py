import time

import dramatiq
import pytest

from dramatiq_redis_streams import StreamsBroker
from dramatiq_redis_streams.keys import GROUP_NAME, dlq_stream_key, queues_key, stream_key

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

class TestConsumerReclaim:
    @pytest.mark.timeout(10)
    def test_recovers_orphaned_messages_past_deadline(self, broker, redis_client):
        broker.enqueue(make_message(args=(99,)))

        # Consumer A reads but never acks — simulates a crash.
        consumer_a = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer_a)
        assert proxy is not None
        consumer_a._closed = True  # prevent any cleanup

        # Let the message become idle.
        time.sleep(1.5)

        # Consumer B whose deadline has already elapsed (no declared time_limit
        # → default_time_limit; set to 0 so the 1.5s-idle message is past it).
        broker_b = StreamsBroker(client=redis_client, middleware=[])
        broker_b.broker_id = "consumer-b"
        consumer_b = broker_b.consume("test-queue", timeout=500)
        consumer_b.reclaim_interval = 0      # sweep on every call
        consumer_b.default_time_limit = 0
        consumer_b.reclaim_grace = 0

        proxy2 = next(consumer_b)
        assert proxy2 is not None
        assert proxy2.args == (99,)
        consumer_b.close()

    @pytest.mark.timeout(10)
    def test_does_not_reclaim_within_task_deadline(self, broker, redis_client):
        """A worker still within a task's time_limit is never robbed of it."""
        broker.enqueue(make_message(args=(7,)))
        consumer_a = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer_a)
        assert proxy is not None
        consumer_a._closed = True

        time.sleep(1.5)  # idle ~1.5s, but the deadline is far larger

        broker_b = StreamsBroker(client=redis_client, middleware=[])
        broker_b.broker_id = "consumer-b"
        consumer_b = broker_b.consume("test-queue", timeout=300)
        consumer_b.reclaim_interval = 0
        consumer_b.default_time_limit = 3_600_000  # 1h deadline
        consumer_b.reclaim_grace = 0
        consumer_b.reclaim_min_idle = 0

        # Nothing reclaimed: the message is well within its deadline.
        assert next(consumer_b) is None
        sk = stream_key("test-queue", broker.namespace)
        owners = {
            (c["name"].decode() if isinstance(c["name"], bytes) else c["name"]): c["pending"]
            for c in redis_client.xinfo_consumers(sk, GROUP_NAME)
        }
        assert owners.get(consumer_a._consumer_name) == 1   # still held by A
        consumer_b.close()

    @pytest.mark.timeout(10)
    def test_reclaim_is_bounded_by_prefetch(self, broker, redis_client):
        """A sweep claims at most `prefetch` orphans, not the whole backlog."""
        for i in range(5):
            broker.enqueue(make_message(args=(i,)))
        dead = broker.consume("test-queue", prefetch=5, timeout=500)
        held = [next(dead) for _ in range(5)]   # dead worker holds all 5
        assert all(m is not None for m in held)
        dead._closed = True

        broker_b = StreamsBroker(client=redis_client, middleware=[])
        broker_b.broker_id = "consumer-b"
        live = broker_b.consume("test-queue", prefetch=2, timeout=300)
        live.reclaim_interval = 0
        live.default_time_limit = 0   # everything past deadline
        live.reclaim_grace = 0
        live.reclaim_min_idle = 0

        live._reclaim_orphans()
        assert len(live._buffer) == 2   # capped at prefetch, not 5
        live.close()

    @pytest.mark.timeout(10)
    def test_does_not_reclaim_own_overdue_message(self, broker, redis_client):
        """A worker must never re-claim its OWN in-flight message, even when it
        has overrun its deadline — that would double-run the task."""
        broker.enqueue(make_message(args=(5,)))
        consumer = broker.consume("test-queue", timeout=300)
        consumer.reclaim_interval = 0
        consumer.default_time_limit = 0   # deadline already elapsed
        consumer.reclaim_grace = 0
        consumer.reclaim_min_idle = 0

        proxy = next(consumer)            # now owned (pending) by this consumer
        assert proxy is not None
        time.sleep(0.1)

        # A sweep must not hand the message back to this same worker.
        consumer._reclaim_orphans()
        assert consumer._buffer == []

        consumer.ack(proxy)
        consumer.close()

    def test_deadline_uses_message_time_limit(self, broker):
        consumer = broker.consume("test-queue", timeout=100)
        msg = make_message()
        msg.options["time_limit"] = 120000
        assert consumer._task_deadline_ms(msg) == 120000 + consumer.reclaim_grace
        consumer.close()

    def test_deadline_falls_back_to_default(self, broker):
        consumer = broker.consume("test-queue", timeout=100)
        consumer.default_time_limit = 45000
        msg = make_message()  # no time_limit declared
        assert consumer._task_deadline_ms(msg) == 45000 + consumer.reclaim_grace
        consumer.close()


class TestConsumerGroupRecovery:
    def test_recreates_missing_group(self, broker, redis_client):
        """If the group is removed out-of-band (e.g. queue removed in the
        dashboard), the consumer recreates and re-registers it instead of
        spinning on NOGROUP errors."""
        broker.declare_queue("work")
        consumer = broker.consume("work", timeout=200)
        sk = stream_key("work", broker.namespace)
        redis_client.xgroup_destroy(sk, GROUP_NAME)

        # Heals rather than raising; returns None this cycle.
        assert next(consumer) is None

        groups = {
            (g.get("name").decode() if isinstance(g.get("name"), bytes) else g.get("name"))
            for g in redis_client.xinfo_groups(sk)
        }
        assert GROUP_NAME in groups
        members = {
            m.decode() if isinstance(m, bytes) else m
            for m in redis_client.smembers(queues_key(broker.namespace))
        }
        assert "work" in members
        consumer.close()
