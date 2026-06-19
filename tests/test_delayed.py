import dramatiq

from dramatiq_redis_streams.delayed import DelayedScheduler
from dramatiq_redis_streams.keys import GROUP_NAME, delayed_key, stream_key

from .conftest import make_message, wait_for


def _consumer_names(redis_client, queue, namespace="dramatiq"):
    consumers = redis_client.xinfo_consumers(stream_key(queue, namespace), GROUP_NAME)
    return {c["name"].decode() if isinstance(c["name"], bytes) else c["name"] for c in consumers}


# ---------------------------------------------------------------------------
# DelayedScheduler
# ---------------------------------------------------------------------------

class TestDelayedScheduler:
    def test_delivers_after_delay(self, broker, redis_client):
        broker.declare_queue("test-queue")
        broker.enqueue(make_message(args=("delayed",)), delay=200)

        assert redis_client.zcard(delayed_key()) == 1
        assert redis_client.xlen(stream_key("test-queue")) == 0

        scheduler = DelayedScheduler(broker, interval=0.05)
        scheduler.start()
        try:
            ok = wait_for(lambda: redis_client.xlen(stream_key("test-queue")) > 0)
            assert ok, "Delayed message was not moved to stream in time"
            assert redis_client.zcard(delayed_key()) == 0
        finally:
            scheduler.stop()
            scheduler.join(timeout=3)

    def test_does_not_deliver_early(self, broker, redis_client):
        broker.declare_queue("test-queue")
        broker.enqueue(make_message(), delay=5000)

        scheduler = DelayedScheduler(broker, interval=0.05)
        scheduler.start()
        try:
            ok = wait_for(lambda: redis_client.xlen(stream_key("test-queue")) > 0, timeout=0.3)
            assert not ok, "Message delivered before its delay elapsed"
            assert redis_client.zcard(delayed_key()) == 1
        finally:
            scheduler.stop()
            scheduler.join(timeout=3)

    def test_ordering(self, broker, redis_client):
        broker.declare_queue("test-queue")
        broker.enqueue(make_message(args=("later",)), delay=500)
        broker.enqueue(make_message(args=("sooner",)), delay=100)

        scheduler = DelayedScheduler(broker, interval=0.05)
        scheduler.start()
        try:
            # Wait for at least the first message.
            ok = wait_for(lambda: redis_client.xlen(stream_key("test-queue")) >= 1)
            assert ok

            entries = redis_client.xrange(stream_key("test-queue"))
            first = dramatiq.Message.decode(
                entries[0][1].get(b"data") or entries[0][1].get("data")
            )
            assert first.args == ("sooner",)

            # Wait for the second.
            ok = wait_for(lambda: redis_client.xlen(stream_key("test-queue")) >= 2)
            assert ok
        finally:
            scheduler.stop()
            scheduler.join(timeout=3)

    def test_multiple_queues(self, broker, redis_client):
        broker.declare_queue("q-a")
        broker.declare_queue("q-b")
        broker.enqueue(make_message(queue="q-a", args=("a",)), delay=100)
        broker.enqueue(make_message(queue="q-b", args=("b",)), delay=100)

        scheduler = DelayedScheduler(broker, interval=0.05)
        scheduler.start()
        try:
            ok = wait_for(
                lambda: (
                    redis_client.xlen(stream_key("q-a")) >= 1
                    and redis_client.xlen(stream_key("q-b")) >= 1
                )
            )
            assert ok, "Delayed messages not delivered to both queues"
        finally:
            scheduler.stop()
            scheduler.join(timeout=3)

    def test_batch_processing(self, broker, redis_client):
        broker.declare_queue("test-queue")
        for i in range(10):
            broker.enqueue(make_message(args=(i,)), delay=50)

        scheduler = DelayedScheduler(broker, interval=0.05, batch_size=5)
        scheduler.start()
        try:
            ok = wait_for(lambda: redis_client.xlen(stream_key("test-queue")) == 10)
            assert ok, f"Only {redis_client.xlen(stream_key('test-queue'))}/10 moved"
        finally:
            scheduler.stop()
            scheduler.join(timeout=3)

    def test_stop(self, broker):
        scheduler = DelayedScheduler(broker, interval=0.1)
        scheduler.start()
        assert scheduler.is_alive()

        scheduler.stop()
        scheduler.join(timeout=3)
        assert not scheduler.is_alive()


# ---------------------------------------------------------------------------
# Stale-consumer reaper
# ---------------------------------------------------------------------------

class TestConsumerReaper:
    def test_reaps_drained_idle_consumer(self, broker, redis_client):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        consumer = broker.consume("work", prefetch=1, timeout=500)
        msg = next(consumer)
        consumer.ack(msg)  # pending now 0
        name = consumer._consumer_name
        assert name in _consumer_names(redis_client, "work")

        # reap_min_idle_ms=0 → any drained consumer is eligible immediately.
        sched = DelayedScheduler(broker, reap_min_idle_ms=0)
        reaped = sched._reap_consumers()

        assert reaped >= 1
        assert name not in _consumer_names(redis_client, "work")
        consumer.close()

    def test_does_not_reap_consumer_with_pending(self, broker, redis_client):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        consumer = broker.consume("work", prefetch=1, timeout=500)
        msg = next(consumer)  # delivered, NOT acked → pending 1
        name = consumer._consumer_name

        sched = DelayedScheduler(broker, reap_min_idle_ms=0)
        reaped = sched._reap_consumers()

        assert reaped == 0
        assert name in _consumer_names(redis_client, "work")
        consumer.ack(msg)
        consumer.close()

    def test_does_not_reap_recently_active_consumer(self, broker, redis_client):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        consumer = broker.consume("work", prefetch=1, timeout=500)
        msg = next(consumer)
        consumer.ack(msg)  # drained, but just now → low idle
        name = consumer._consumer_name

        # High idle threshold → a just-active consumer is not eligible.
        sched = DelayedScheduler(broker, reap_min_idle_ms=3_600_000)
        reaped = sched._reap_consumers()

        assert reaped == 0
        assert name in _consumer_names(redis_client, "work")
        consumer.close()
