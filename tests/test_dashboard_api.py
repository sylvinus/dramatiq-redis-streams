"""Tests for the dashboard data-layer functions."""

import dramatiq

from dramatiq_redis_streams.dashboard import api
from dramatiq_redis_streams.keys import ABANDONED_CONSUMER, dlq_expiry_key, dlq_stream_key, queues_key, stream_key


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

    def test_processed_and_lag_fields(self, broker, redis_client):
        """`processed` counts acked messages; `lag` counts undelivered ones."""
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        broker.enqueue(make_message(queue="work"))

        consumer = broker.consume("work", prefetch=1, timeout=1000)
        msg = next(consumer)
        consumer.ack(msg)

        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        q = next(q for q in result["queues"] if q["name"] == "work")
        # One message acked → processed == 1; one never delivered → lag == 1.
        assert q["processed"] == 1
        assert q["lag"] == 1

        consumer.close()

    def test_lag_is_none_without_consumer_group(self, broker, redis_client):
        """With no consumer group yet, lag is unavailable → reported as None."""
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))  # creates stream, no group
        result = api.get_overview(redis_client, broker.namespace, broker.get_declared_queues())
        q = next(q for q in result["queues"] if q["name"] == "work")
        assert q["lag"] is None

    def test_discovers_undeclared_queues(self, broker, redis_client):
        """Queues registered by other processes (in the registry set) are
        discovered without this process declaring them."""
        redis_client.sadd(queues_key(broker.namespace), "other-process-queue")
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

    def test_surfaces_error_and_retries(self, broker, redis_client):
        """The failure traceback and retry count are surfaced for triage."""
        dk = dlq_stream_key("work", broker.namespace)
        msg = make_message(queue="work", actor="boom")
        msg.options["traceback"] = "Traceback (most recent call last):\n ...\nValueError: boom"
        msg.options["retries"] = 3
        redis_client.xadd(dk, {"data": msg.encode()})
        result = api.get_dlq_messages(redis_client, broker.namespace, "work")
        assert "ValueError: boom" in result[0]["error"]
        assert result[0]["retries"] == 3


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


class TestRequeueAllDlq:
    def test_requeue_all_moves_everything_in_order(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        sk = stream_key("work", broker.namespace)
        for i in range(5):
            redis_client.xadd(dk, {"data": make_message(queue="work", actor=f"a{i}").encode()})

        count = api.requeue_all_dlq(redis_client, broker.namespace, "work")
        assert count == 5
        assert redis_client.xlen(dk) == 0
        assert redis_client.xlen(sk) == 5

        # FIFO order preserved (a0..a4).
        actors = [
            dramatiq.Message.decode(edata.get(b"data") or edata.get("data")).actor_name
            for _eid, edata in redis_client.xrange(sk)
        ]
        assert actors == [f"a{i}" for i in range(5)]

    def test_requeue_all_handles_batches(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        sk = stream_key("work", broker.namespace)
        for i in range(12):
            redis_client.xadd(dk, {"data": make_message(queue="work", actor=f"a{i}").encode()})
        count = api.requeue_all_dlq(redis_client, broker.namespace, "work", batch=5)
        assert count == 12
        assert redis_client.xlen(dk) == 0
        assert redis_client.xlen(sk) == 12

    def test_requeue_all_empty(self, broker, redis_client):
        count = api.requeue_all_dlq(redis_client, broker.namespace, "work")
        assert count == 0


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

    def test_purge_clears_expiry_index(self, broker, redis_client):
        dk = dlq_stream_key("work", broker.namespace)
        ek = dlq_expiry_key("work", broker.namespace)
        redis_client.xadd(dk, {"data": b"x"})
        redis_client.zadd(ek, {"1-0": 12345})
        api.purge_dlq(redis_client, broker.namespace, "work")
        assert redis_client.exists(ek) == 0


class TestFlushQueue:
    def test_flush(self, broker, redis_client):
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        broker.enqueue(make_message(queue="work"))
        sk = stream_key("work", broker.namespace)
        assert redis_client.xlen(sk) == 2
        api.flush_queue(redis_client, broker.namespace, "work")
        assert redis_client.xlen(sk) == 0


class TestRemoveQueue:
    def test_removes_stream_dlq_and_registry(self, broker, redis_client):
        redis_client.sadd(queues_key(broker.namespace), "work")
        redis_client.xadd(stream_key("work", broker.namespace), {"data": b"x"})
        redis_client.xadd(dlq_stream_key("work", broker.namespace), {"data": b"y"})

        assert api.remove_queue(redis_client, broker.namespace, "work") is True
        assert not redis_client.exists(stream_key("work", broker.namespace))
        assert not redis_client.exists(dlq_stream_key("work", broker.namespace))
        members = redis_client.smembers(queues_key(broker.namespace))
        assert b"work" not in members and "work" not in members


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

    def test_abandoned_sentinel_hidden(self, broker, redis_client):
        """The abandoned-message sentinel consumer is not shown as a worker."""
        broker.declare_queue("work")
        for i in range(3):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=3, timeout=100)
        next(consumer)        # reads 3, returns 1; 2 remain buffered
        consumer.close()      # abandons the 2 → creates the sentinel consumer

        result = api.get_workers(redis_client, broker.namespace, broker.get_declared_queues())
        names = [w["name"] for w in result]
        assert ABANDONED_CONSUMER not in names
        assert any(n.startswith("worker-") for n in names)

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

    def test_pending_messages_are_capped(self, broker, redis_client):
        """Detail list is capped while aggregate counts stay exact."""
        broker.declare_queue("work")
        for i in range(25):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=25, timeout=1000)
        msgs = [next(consumer) for _ in range(25)]

        result = api.get_workers(
            redis_client, broker.namespace, broker.get_declared_queues(),
            pending_limit=10,
        )
        worker = next(w for w in result if w["name"].startswith("worker-"))
        # Aggregate count reflects everything the worker owns...
        assert worker["total_pending"] == 25
        # ...but the detail list is capped.
        assert len(worker["pending_messages"]) == 10
        assert worker["pending_messages"][0]["actor"].startswith("a")

        for m in msgs:
            consumer.ack(m)
        consumer.close()

    def test_pending_limit_zero(self, broker, redis_client):
        """pending_limit=0 skips the expensive detail fetch entirely."""
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        consumer = broker.consume("work", prefetch=1, timeout=1000)
        msg = next(consumer)

        result = api.get_workers(
            redis_client, broker.namespace, broker.get_declared_queues(),
            pending_limit=0,
        )
        worker = next(w for w in result if w["name"].startswith("worker-"))
        assert worker["total_pending"] == 1
        assert worker["pending_messages"] == []

        consumer.ack(msg)
        consumer.close()


def _id_key(stream_id):
    """Sort key matching Redis stream-ID ordering: numeric (ms, seq), not the
    lexicographic order Python's default string sort would give (which puts
    '...-10' before '...-2')."""
    ms, _, seq = stream_id.partition("-")
    return (int(ms), int(seq))


class TestWorkerPendingPagination:
    def _worker_name(self, redis_client, broker, declared):
        result = api.get_workers(redis_client, broker.namespace, declared)
        return next(w["name"] for w in result if w["name"].startswith("worker-"))

    def test_walks_entire_pel_in_pages(self, broker, redis_client):
        """Seek pagination returns every pending task exactly once, in order."""
        broker.declare_queue("work")
        for i in range(25):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=25, timeout=1000)
        msgs = [next(consumer) for _ in range(25)]
        declared = broker.get_declared_queues()
        wname = self._worker_name(redis_client, broker, declared)

        seen, cursor, pages = [], None, 0
        while True:
            page = api.get_worker_pending(
                redis_client, broker.namespace, wname, declared,
                after=cursor, count=10,
            )
            seen.extend(m["id"] for m in page["messages"])
            pages += 1
            assert pages < 10  # guard against a cursor that never terminates
            cursor = page["next_cursor"]
            if cursor is None:
                break

        assert len(seen) == 25                      # every pending task...
        assert len(set(seen)) == 25                 # ...exactly once (no overlap)
        assert seen == sorted(seen, key=_id_key)    # ascending stream-id order
        assert pages == 3                           # 10 + 10 + 5

        for m in msgs:
            consumer.ack(m)
        consumer.close()

    def test_continues_from_get_workers_cursor(self, broker, redis_client):
        """The cursor embedded in get_workers resumes after its first page."""
        broker.declare_queue("work")
        for i in range(25):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=25, timeout=1000)
        msgs = [next(consumer) for _ in range(25)]
        declared = broker.get_declared_queues()

        workers = api.get_workers(
            redis_client, broker.namespace, declared, pending_limit=10,
        )
        worker = next(w for w in workers if w["name"].startswith("worker-"))
        first_ids = [m["id"] for m in worker["pending_messages"]]
        assert len(first_ids) == 10
        assert worker["pending_cursor"] is not None

        rest, cursor = [], worker["pending_cursor"]
        while cursor is not None:
            page = api.get_worker_pending(
                redis_client, broker.namespace, worker["name"], declared,
                after=cursor, count=10,
            )
            rest.extend(m["id"] for m in page["messages"])
            cursor = page["next_cursor"]

        assert len(rest) == 15                          # the remaining tasks
        assert set(first_ids).isdisjoint(rest)          # no overlap with page one
        # First page then the rest is the full PEL in stream-id order.
        assert first_ids + rest == sorted(first_ids + rest, key=_id_key)

        for m in msgs:
            consumer.ack(m)
        consumer.close()

    def test_no_cursor_when_not_truncated(self, broker, redis_client):
        """A fully-shown worker has no cursor, and its page has no next page."""
        broker.declare_queue("work")
        broker.enqueue(make_message(queue="work"))
        consumer = broker.consume("work", prefetch=1, timeout=1000)
        msg = next(consumer)
        declared = broker.get_declared_queues()

        worker = next(
            w for w in api.get_workers(redis_client, broker.namespace, declared)
            if w["name"].startswith("worker-")
        )
        assert worker["pending_cursor"] is None

        page = api.get_worker_pending(
            redis_client, broker.namespace, worker["name"], declared, count=10,
        )
        assert len(page["messages"]) == 1
        assert page["next_cursor"] is None

        consumer.ack(msg)
        consumer.close()

    def test_paginates_across_multiple_queues(self, broker, redis_client):
        """A page boundary that falls between two queues is handled cleanly."""
        broker.declare_queue("q1")
        broker.declare_queue("q2")
        for i in range(5):
            broker.enqueue(make_message(queue="q1", actor=f"x{i}"))
        for i in range(5):
            broker.enqueue(make_message(queue="q2", actor=f"y{i}"))
        c1 = broker.consume("q1", prefetch=5, timeout=1000)
        c2 = broker.consume("q2", prefetch=5, timeout=1000)
        m1 = [next(c1) for _ in range(5)]
        m2 = [next(c2) for _ in range(5)]
        declared = broker.get_declared_queues()
        wname = self._worker_name(redis_client, broker, declared)

        seen, cursor = [], None
        while True:
            page = api.get_worker_pending(
                redis_client, broker.namespace, wname, declared,
                after=cursor, count=3,  # small page → boundary crosses queues
            )
            seen.extend((m["queue"], m["id"]) for m in page["messages"])
            cursor = page["next_cursor"]
            if cursor is None:
                break

        assert len(seen) == 10
        assert len(set(seen)) == 10                 # each entry once
        queues_seen = [q for q, _ in seen]
        assert queues_seen == sorted(queues_seen)   # all q1, then all q2
        assert queues_seen.count("q1") == 5
        assert queues_seen.count("q2") == 5

        for m in m1:
            c1.ack(m)
        for m in m2:
            c2.ack(m)
        c1.close()
        c2.close()

    def test_bad_cursor_starts_from_beginning(self, broker, redis_client):
        """A malformed cursor degrades to the first page rather than erroring."""
        broker.declare_queue("work")
        for i in range(3):
            broker.enqueue(make_message(queue="work", actor=f"a{i}"))
        consumer = broker.consume("work", prefetch=3, timeout=1000)
        msgs = [next(consumer) for _ in range(3)]
        declared = broker.get_declared_queues()
        wname = self._worker_name(redis_client, broker, declared)

        page = api.get_worker_pending(
            redis_client, broker.namespace, wname, declared,
            after="not-a-real-cursor", count=10,
        )
        assert len(page["messages"]) == 3

        for m in msgs:
            consumer.ack(m)
        consumer.close()

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
