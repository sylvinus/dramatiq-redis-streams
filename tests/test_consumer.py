import time

import dramatiq
import pytest

from dramatiq_redis_streams import StreamsBroker
from dramatiq_redis_streams.keys import GROUP_NAME, dlq_expiry_key, dlq_stream_key, queues_key, stream_key

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

    @pytest.mark.timeout(10)
    def test_respects_prefetch_limit(self, broker, redis_client):
        """A worker reserves at most `prefetch` messages; the rest stay
        available for other workers (so scaling out actually works)."""
        for i in range(5):
            broker.enqueue(make_message(args=(i,)))

        consumer = broker.consume("test-queue", prefetch=2, timeout=100)
        got = []
        for _ in range(3):  # 2 messages, then one None at the prefetch limit
            m = next(consumer)
            if m is not None:
                got.append(m)
        assert len(got) == 2  # only prefetch reserved, not the whole queue

        # A second worker can still pick up the remaining 3.
        broker2 = StreamsBroker(client=redis_client, middleware=[])
        broker2.broker_id = "worker-2"
        c2 = broker2.consume("test-queue", prefetch=10, timeout=100)
        got2 = []
        for _ in range(4):
            m = next(c2)
            if m is not None:
                got2.append(m)
        assert len(got2) == 3

        consumer.close()
        c2.close()

    def test_close_abandons_buffered_for_immediate_reclaim(self, broker, redis_client):
        """Clean shutdown marks prefetched-but-unstarted messages maximally idle
        so the next worker reclaims them at once (ahead of the backlog), instead
        of leaving them to wait out their reclaim deadline."""
        for i in range(3):
            broker.enqueue(make_message(args=(i,)))
        consumer = broker.consume("test-queue", prefetch=3, timeout=100)
        first = next(consumer)            # reads all 3 into buffer, returns 1
        assert first is not None          # 2 remain buffered, unhanded
        consumer.close()                  # abandons those 2 (XCLAIM, huge idle)

        # Another worker's reclaim sweep grabs them immediately, despite the
        # task deadline being far in the future.
        broker2 = StreamsBroker(client=redis_client, middleware=[])
        broker2.broker_id = "worker-2"
        c2 = broker2.consume("test-queue", prefetch=10, timeout=100)
        c2.reclaim_min_idle = 0
        c2._reclaim_orphans()

        got = []
        while c2._buffer:
            got.append(c2._buffer.pop(0))
        assert sorted(m.args[0] for m in got) == [1, 2]
        c2.close()

    def test_abandoned_reclaimable_by_same_worker_name(self, broker, redis_client):
        """dramatiq also calls close() on the error path, then recreates a
        consumer with the SAME name. Abandoned messages must still be reclaimable
        by it — so they're owned by a sentinel, not the worker (which would skip
        its own pending)."""
        for i in range(3):
            broker.enqueue(make_message(args=(i,)))
        c1 = broker.consume("test-queue", prefetch=3, timeout=100)
        assert next(c1) is not None       # returns args=0; buffer = args [1, 2]
        c1.close()                        # abandons [1, 2]

        # Same broker id → same consumer name as c1.
        c2 = broker.consume("test-queue", prefetch=10, timeout=100)
        assert c2._consumer_name == c1._consumer_name
        c2.reclaim_min_idle = 0
        c2._reclaim_orphans()

        got = []
        while c2._buffer:
            got.append(c2._buffer.pop(0))
        assert sorted(m.args[0] for m in got) == [1, 2]
        c2.close()

    def test_zero_prefetch_does_not_wedge(self, broker):
        """A non-positive prefetch is clamped to 1, never stuck at zero room."""
        broker.enqueue(make_message(args=(1,)))
        consumer = broker.consume("test-queue", prefetch=0, timeout=100)
        assert consumer.prefetch == 1
        m = next(consumer)
        assert m is not None
        consumer.ack(m)
        consumer.close()

    def test_nack_frees_prefetch_capacity(self, broker):
        """nack releases a slot just like ack."""
        for i in range(2):
            broker.enqueue(make_message(args=(i,)))
        consumer = broker.consume("test-queue", prefetch=1, timeout=100)
        m1 = next(consumer)
        assert next(consumer) is None     # at capacity
        consumer.nack(m1)                 # → DLQ, and frees the slot
        m2 = next(consumer)
        assert m2 is not None and m2.args != m1.args
        consumer.ack(m2)
        consumer.close()

    def test_requeue_frees_prefetch_capacity(self, broker):
        """requeue releases the slots of the messages it returns."""
        broker.enqueue(make_message(args=(1,)))
        broker.enqueue(make_message(args=(2,)))
        consumer = broker.consume("test-queue", prefetch=1, timeout=100)
        m1 = next(consumer)
        assert next(consumer) is None     # at capacity
        consumer.requeue([m1])            # frees the slot
        m = next(consumer)
        assert m is not None
        consumer.ack(m)
        consumer.close()

    def test_inherits_pending_on_restart(self, broker, redis_client):
        """A recreated consumer (same worker id) accounts for the PEL it
        inherits, so the prefetch limit stays correct across restarts."""
        for i in range(3):
            broker.enqueue(make_message(args=(i,)))
        c1 = broker.consume("test-queue", prefetch=3, timeout=100)
        held = [next(c1) for _ in range(3)]
        assert all(m is not None for m in held)  # c1 now holds 3 in its PEL

        # Simulate a restart: a fresh consumer for the same worker.
        c2 = broker.consume("test-queue", prefetch=3, timeout=100)
        assert c2._unacked == 3  # inherited the PEL, won't over-reserve

        for m in held:
            c1.ack(m)
        c1.close()
        c2.close()

    def test_ack_frees_prefetch_capacity(self, broker):
        """Acking a message lets the worker reserve another (capacity reopens)."""
        for i in range(3):
            broker.enqueue(make_message(args=(i,)))
        consumer = broker.consume("test-queue", prefetch=1, timeout=100)

        m1 = next(consumer)
        assert m1 is not None
        assert next(consumer) is None  # at capacity (prefetch=1), nothing more
        consumer.ack(m1)
        m2 = next(consumer)  # slot freed → next message delivered
        assert m2 is not None and m2.args != m1.args
        consumer.ack(m2)
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

    def _dead_message(self, redis_client, queue="test-queue"):
        entries = redis_client.xrange(dlq_stream_key(queue))
        raw = entries[0][1].get(b"data") or entries[0][1].get("data")
        return dramatiq.Message.decode(raw)

    def test_keeps_existing_traceback(self, broker, redis_client):
        """A traceback already recorded by Retries rides along to the DLQ."""
        msg = make_message()
        msg.options["traceback"] = "ValueError: boom"
        broker.enqueue(msg)
        consumer = broker.consume("test-queue", timeout=1000)
        consumer.nack(next(consumer))
        assert self._dead_message(redis_client).options.get("traceback") == "ValueError: boom"
        consumer.close()

    def test_records_exception_when_no_traceback(self, broker, redis_client):
        """Fallback (e.g. ActorNotFound): the stuffed exception is recorded."""
        broker.enqueue(make_message())
        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        proxy.stuff_exception(ValueError("kaboom"))   # no options["traceback"]
        consumer.nack(proxy)
        assert "kaboom" in self._dead_message(redis_client).options.get("traceback")
        consumer.close()

    def test_truncates_huge_traceback(self, broker, redis_client):
        """A failure reason can't balloon the DLQ entry."""
        msg = make_message()
        msg.options["traceback"] = "X" * 50000
        broker.enqueue(msg)
        consumer = broker.consume("test-queue", timeout=1000)
        consumer.nack(next(consumer))
        tb = self._dead_message(redis_client).options.get("traceback")
        assert len(tb) < 9000   # hard-capped (8192 + truncation marker)
        consumer.close()

    def test_schedules_expiry_from_message_ttl(self, broker, redis_client):
        """A per-message dead_message_ttl indexes the dead-letter for deletion."""
        msg = make_message()
        msg.options["dead_message_ttl"] = 60000
        broker.enqueue(msg)
        consumer = broker.consume("test-queue", timeout=1000)
        consumer.nack(next(consumer))
        assert redis_client.zcard(dlq_expiry_key("test-queue", broker.namespace)) == 1
        consumer.close()

    def test_default_ttl_applies(self, broker, redis_client):
        """With no per-task value, the broker default (7 days) is used."""
        broker.enqueue(make_message())
        consumer = broker.consume("test-queue", timeout=1000)
        consumer.nack(next(consumer))
        ek = dlq_expiry_key("test-queue", broker.namespace)
        assert redis_client.zcard(ek) == 1
        score = redis_client.zrange(ek, 0, 0, withscores=True)[0][1]
        assert score > time.time() * 1000 + 6 * 86_400_000   # ~7 days out
        consumer.close()

    def test_zero_ttl_keeps_forever(self, broker, redis_client):
        """dead_message_ttl=0 opts out of expiry even when a default is set."""
        msg = make_message()
        msg.options["dead_message_ttl"] = 0
        broker.enqueue(msg)
        consumer = broker.consume("test-queue", timeout=1000)
        consumer.nack(next(consumer))
        assert redis_client.zcard(dlq_expiry_key("test-queue", broker.namespace)) == 0
        consumer.close()

    def test_dead_message_ttl_from_actor_option(self, broker, redis_client):
        """`@actor(dead_message_ttl=...)` is accepted and resolved at nack time."""
        dramatiq.set_broker(broker)

        @dramatiq.actor(queue_name="test-queue", dead_message_ttl=120000)
        def boom():
            pass

        assert "dead_message_ttl" in broker.actor_options
        broker.enqueue(boom.message())
        consumer = broker.consume("test-queue", timeout=1000)
        consumer.nack(next(consumer))
        assert redis_client.zcard(dlq_expiry_key("test-queue", broker.namespace)) == 1
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
    def test_reclaim_purges_deleted_pending_entry(self, broker, redis_client):
        """A pending entry whose payload was deleted from the stream (e.g.
        trimmed) is purged from the PEL by the reclaim sweep, so it can't leave
        the owning consumer permanently un-reapable."""
        broker.enqueue(make_message(args=(1,)))
        sk = stream_key("test-queue", broker.namespace)
        c1 = broker.consume("test-queue", prefetch=1, timeout=100)
        m = next(c1)                                   # delivered → in c1's PEL
        redis_client.xdel(sk, m._redis_stream_id)      # gone from stream, still pending
        c1._closed = True

        broker2 = StreamsBroker(client=redis_client, middleware=[])
        broker2.broker_id = "worker-2"
        c2 = broker2.consume("test-queue", prefetch=10, timeout=100)
        c2.reclaim_min_idle = 0
        c2._reclaim_orphans()

        assert c2._buffer == []                        # nothing to process
        assert redis_client.xpending(sk, GROUP_NAME)["pending"] == 0  # PEL purged
        c2.close()

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
