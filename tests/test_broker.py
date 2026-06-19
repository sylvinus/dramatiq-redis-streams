import dramatiq
import pytest

from dramatiq_redis_streams import StreamsBroker
from dramatiq_redis_streams.keys import GROUP_NAME, delayed_key, queues_key, stream_key

from .conftest import make_message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _group_names(redis_client, key):
    """Return the set of consumer-group names on *key*."""
    groups = redis_client.xinfo_groups(key)
    return {g.get("name") or g.get(b"name") for g in groups}


# ---------------------------------------------------------------------------
# declare_queue
# ---------------------------------------------------------------------------

class TestDeclareQueue:
    def test_does_not_touch_redis(self, broker, redis_client):
        """Declaring a queue is purely in-memory — no eager Redis I/O.

        This is what lets task modules be imported (which declares actors and
        thus queues) without a running Redis.
        """
        broker.declare_queue("test-queue")
        # Neither the stream nor the consumer group is created yet.
        assert not redis_client.exists(stream_key("test-queue"))
        assert not redis_client.exists(stream_key("test-queue.DQ"))

    def test_idempotent(self, broker):
        broker.declare_queue("test-queue")
        broker.declare_queue("test-queue")  # should not raise
        assert "test-queue" in broker.queues

    def test_adds_to_queues_set(self, broker):
        broker.declare_queue("test-queue")
        assert "test-queue" in broker.queues

    def test_multiple_queues(self, broker):
        for name in ["alpha", "beta", "gamma"]:
            broker.declare_queue(name)
        assert {"alpha", "beta", "gamma"} <= broker.queues


class TestTaskTimeout:
    def test_default_time_limit_aligns_timelimit_middleware(self, redis_client):
        """default_time_limit drives dramatiq's in-worker TimeLimit abort too."""
        from dramatiq.middleware import TimeLimit

        b = StreamsBroker(client=redis_client, default_time_limit=12345)
        try:
            tl = next(m for m in b.middleware if isinstance(m, TimeLimit))
            assert tl.time_limit == 12345
        finally:
            b.close()

    def test_custom_middleware_is_left_untouched(self, redis_client):
        """A caller's TimeLimit is not mutated, and the reclaim deadline follows
        it (so the in-worker abort and reclaim can't diverge)."""
        from dramatiq.middleware import TimeLimit

        tl = TimeLimit(time_limit=999)
        b = StreamsBroker(client=redis_client, middleware=[tl], default_time_limit=12345)
        try:
            assert tl.time_limit == 999            # not mutated
            assert b.default_time_limit == 999     # reclaim follows it
        finally:
            b.close()

    def test_custom_middleware_without_timelimit(self, redis_client):
        """With no TimeLimit in custom middleware, reclaim uses the param."""
        b = StreamsBroker(client=redis_client, middleware=[], default_time_limit=12345)
        try:
            assert b.default_time_limit == 12345
        finally:
            b.close()


class TestQueueRegistry:
    def test_consume_registers_queue(self, broker, redis_client):
        """Workers register the queues they consume in the shared registry."""
        consumer = broker.consume("test-queue", timeout=100)
        members = {
            m.decode() if isinstance(m, bytes) else m
            for m in redis_client.smembers(queues_key(broker.namespace))
        }
        assert "test-queue" in members
        consumer.close()

    def test_declare_queue_does_not_register(self, broker, redis_client):
        """Declaring alone is Redis-free, so it must not touch the registry."""
        broker.declare_queue("test-queue")
        assert not redis_client.exists(queues_key(broker.namespace))


class TestConsumeCreatesGroup:
    def test_consume_creates_group(self, broker, redis_client):
        """The consumer group is created lazily at consume() time."""
        consumer = broker.consume("test-queue", timeout=100)
        names = _group_names(redis_client, stream_key("test-queue"))
        assert GROUP_NAME.encode() in names or GROUP_NAME in names
        consumer.close()

    def test_messages_enqueued_before_consumer_are_delivered(self, broker, redis_client):
        """Group is created at id "0", so pre-consumer messages aren't lost."""
        broker.enqueue(make_message(actor="early"))
        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        assert proxy is not None
        assert proxy.actor_name == "early"
        consumer.ack(proxy)
        consumer.close()


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

class TestEnqueue:
    def test_adds_to_stream(self, broker, redis_client):
        broker.enqueue(make_message())
        assert redis_client.xlen(stream_key("test-queue")) == 1

    def test_returns_message(self, broker):
        msg = make_message()
        result = broker.enqueue(msg)
        assert result is msg

    def test_message_data_roundtrips(self, broker, redis_client):
        msg = make_message(args=(1, 2), kwargs={"k": "v"})
        broker.enqueue(msg)

        entries = redis_client.xrange(stream_key("test-queue"))
        raw = entries[0][1].get(b"data") or entries[0][1].get("data")
        decoded = dramatiq.Message.decode(raw)
        assert decoded.args == (1, 2)
        assert decoded.kwargs == {"k": "v"}

    def test_with_delay_uses_sorted_set(self, broker, redis_client):
        broker.enqueue(make_message(), delay=5000)
        assert redis_client.xlen(stream_key("test-queue")) == 0
        assert redis_client.zcard(delayed_key()) == 1

    def test_multiple_enqueues(self, broker, redis_client):
        for _ in range(5):
            broker.enqueue(make_message())
        assert redis_client.xlen(stream_key("test-queue")) == 5


# ---------------------------------------------------------------------------
# flush / flush_all
# ---------------------------------------------------------------------------

class TestFlush:
    def test_clears_stream(self, broker, redis_client):
        broker.enqueue(make_message())
        broker.flush("test-queue")
        assert redis_client.xlen(stream_key("test-queue")) == 0

    def test_preserves_group(self, broker, redis_client):
        broker.declare_queue("test-queue")
        broker.flush("test-queue")
        names = _group_names(redis_client, stream_key("test-queue"))
        assert GROUP_NAME.encode() in names or GROUP_NAME in names


class TestFlushAll:
    def test_clears_all_queues(self, broker, redis_client):
        for q in ["q1", "q2"]:
            broker.enqueue(make_message(queue=q))
        broker.flush_all()
        for q in ["q1", "q2"]:
            assert redis_client.xlen(stream_key(q)) == 0

    def test_clears_delayed_set(self, broker, redis_client):
        broker.enqueue(make_message(), delay=5000)
        broker.flush_all()
        assert redis_client.zcard(delayed_key()) == 0


# ---------------------------------------------------------------------------
# join
# ---------------------------------------------------------------------------

class TestJoin:
    def test_raises_on_timeout(self, broker):
        broker.enqueue(make_message())
        with pytest.raises(dramatiq.QueueJoinTimeout):
            broker.join("test-queue", timeout=200)

    def test_timeout_zero_raises_immediately(self, broker):
        broker.enqueue(make_message())
        with pytest.raises(dramatiq.QueueJoinTimeout):
            broker.join("test-queue", timeout=0)

    def test_returns_immediately_when_empty(self, broker):
        broker.declare_queue("test-queue")
        broker.join("test-queue", timeout=1000)  # should not raise

    def test_returns_after_ack(self, broker):
        broker.enqueue(make_message())
        consumer = broker.consume("test-queue", timeout=1000)
        proxy = next(consumer)
        consumer.ack(proxy)
        broker.join("test-queue", timeout=2000)  # should not raise
        consumer.close()


# ---------------------------------------------------------------------------
# get_declared_queues / get_declared_delay_queues
# ---------------------------------------------------------------------------

class TestGetDeclaredQueues:
    def test_returns_declared_queues(self, broker):
        broker.declare_queue("q1")
        broker.declare_queue("q2")
        assert broker.get_declared_queues() == {"q1", "q2"}

    def test_returns_copy(self, broker):
        broker.declare_queue("q1")
        qs = broker.get_declared_queues()
        qs.add("phantom")
        assert "phantom" not in broker.queues

    def test_no_delay_queues_declared(self, broker):
        """This broker manages delays via its own sorted-set scheduler, so it
        declares no ``.DQ`` queues — which stops the Worker from spawning idle
        delay-queue consumers."""
        broker.declare_queue("q1")
        assert broker.get_declared_delay_queues() == set()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestClose:
    def test_stops_scheduler(self, broker):
        # Start scheduler by consuming
        consumer = broker.consume("test-queue", timeout=100)
        next(consumer)  # triggers scheduler start
        consumer.close()

        assert broker._scheduler_started
        broker.close()
        assert not broker._scheduler_started

    def test_idempotent_close(self, broker):
        broker.close()
        broker.close()  # should not raise
