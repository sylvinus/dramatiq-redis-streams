import dramatiq
import pytest

from dramatiq_redis_streams.keys import GROUP_NAME, delayed_key, stream_key

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
    def test_creates_stream_and_group(self, broker, redis_client):
        broker.declare_queue("test-queue")
        names = _group_names(redis_client, stream_key("test-queue"))
        assert GROUP_NAME.encode() in names or GROUP_NAME in names

    def test_creates_delay_queue_stream(self, broker, redis_client):
        broker.declare_queue("test-queue")
        names = _group_names(redis_client, stream_key("test-queue.DQ"))
        assert GROUP_NAME.encode() in names or GROUP_NAME in names

    def test_idempotent(self, broker, redis_client):
        broker.declare_queue("test-queue")
        broker.declare_queue("test-queue")  # should not raise
        names = _group_names(redis_client, stream_key("test-queue"))
        assert len(names) == 1

    def test_adds_to_queues_set(self, broker):
        broker.declare_queue("test-queue")
        assert "test-queue" in broker.queues

    def test_multiple_queues(self, broker, redis_client):
        for name in ["alpha", "beta", "gamma"]:
            broker.declare_queue(name)
        for name in ["alpha", "beta", "gamma"]:
            assert redis_client.exists(stream_key(name))


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

    def test_delay_queues(self, broker):
        broker.declare_queue("q1")
        assert "q1.DQ" in broker.get_declared_delay_queues()


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
