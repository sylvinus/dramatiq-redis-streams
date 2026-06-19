import logging
import time

import dramatiq
import redis as redis_mod

from .keys import GROUP_NAME, consumer_name, dlq_stream_key, stream_key

logger = logging.getLogger(__name__)


class StreamsConsumer(dramatiq.Consumer):
    """A Dramatiq consumer backed by Redis Streams.

    Uses ``XREADGROUP`` with ``BLOCK`` for efficient, event-driven message
    consumption. Messages orphaned by a dead worker are recovered, but **only
    once they have been unacked for longer than their own task deadline** — a
    worker legitimately running a task within its declared ``time_limit`` is
    never robbed of it.

    Parameters:
        broker: The parent :class:`StreamsBroker`.
        queue_name: Queue to consume from.
        prefetch: Max messages to fetch per ``XREADGROUP`` call.
        timeout: Block timeout in milliseconds.
        reclaim_interval: Milliseconds between orphan-recovery sweeps.
        default_time_limit: Reclaim deadline in ms for messages whose actor or
            message declares no ``time_limit`` (default 60 000 — deliberately
            tight, so a forgotten timeout surfaces quickly).
        reclaim_grace: Extra ms added to each task's deadline before its message
            is eligible for reclaim, covering the gap between a TimeLimit abort
            and the resulting ack/nack.
        reclaim_min_idle: Pending entries idler than this (ms) are candidates;
            cheaply skips freshly-delivered messages. Caps the smallest
            detectable timeout.
        reclaim_batch: Max pending entries inspected per sweep.
    """

    def __init__(
        self,
        *,
        broker,
        queue_name,
        prefetch=1,
        timeout=30000,
        reclaim_interval=30000,
        default_time_limit=60000,
        reclaim_grace=10000,
        reclaim_min_idle=1000,
        reclaim_batch=100,
    ):
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout = timeout
        self.reclaim_interval = reclaim_interval
        self.default_time_limit = default_time_limit
        self.reclaim_grace = reclaim_grace
        self.reclaim_min_idle = reclaim_min_idle
        self.reclaim_batch = reclaim_batch

        self._buffer = []
        self._last_reclaim = 0.0
        self._closed = False
        self._consumer_name = consumer_name(broker.broker_id)
        self._stream_key = stream_key(queue_name, broker.namespace)

    # ------------------------------------------------------------------
    # Iterator protocol
    # ------------------------------------------------------------------

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration

        # 1. Drain local buffer first.
        if self._buffer:
            return self._buffer.pop(0)

        # 2. Periodic sweep to recover messages orphaned by dead workers.
        now = time.monotonic()
        if now - self._last_reclaim >= self.reclaim_interval / 1000:
            self._reclaim_orphans()
            self._last_reclaim = now
            if self._buffer:
                return self._buffer.pop(0)

        # 3. XREADGROUP BLOCK — efficient server-side wait.
        try:
            results = self.broker.client.xreadgroup(
                GROUP_NAME,
                self._consumer_name,
                {self._stream_key: ">"},
                count=self.prefetch,
                block=self.timeout,
            )
        except redis_mod.ResponseError as exc:
            if "NOGROUP" in str(exc):
                # The group was removed out-of-band (e.g. the queue was removed
                # from the dashboard). Recreate it and re-register so an in-use
                # queue heals instead of spinning on errors.
                logger.info("Consumer group missing for %s, recreating", self._stream_key)
                self.broker._ensure_group(self._stream_key)
                self.broker._register_queue(self.queue_name, force=True)
                return None
            logger.warning("Redis error during XREADGROUP, will retry", exc_info=True)
            time.sleep(1)
            return None
        except redis_mod.RedisError:
            logger.warning("Redis error during XREADGROUP, will retry", exc_info=True)
            time.sleep(1)
            return None

        if results:
            for _stream_name, entries in results:
                for entry_id, entry_data in entries:
                    proxy = self._parse_entry(entry_id, entry_data)
                    if proxy is not None:
                        self._buffer.append(proxy)

        if self._buffer:
            return self._buffer.pop(0)

        return None  # timeout, no messages

    # ------------------------------------------------------------------
    # Message lifecycle
    # ------------------------------------------------------------------

    def ack(self, message):
        try:
            stream_id = message._redis_stream_id
            pipe = self.broker.client.pipeline()
            pipe.xack(self._stream_key, GROUP_NAME, stream_id)
            pipe.xdel(self._stream_key, stream_id)
            pipe.execute()
        except redis_mod.RedisError:
            logger.warning("Failed to ack message %s", message.message_id, exc_info=True)

    def nack(self, message):
        try:
            stream_id = message._redis_stream_id
            dlq_key = dlq_stream_key(self.queue_name, self.broker.namespace)

            pipe = self.broker.client.pipeline()
            pipe.xadd(dlq_key, {"data": message.encode()})
            pipe.xack(self._stream_key, GROUP_NAME, stream_id)
            pipe.xdel(self._stream_key, stream_id)
            pipe.execute()
        except redis_mod.RedisError:
            logger.warning("Failed to nack message %s", message.message_id, exc_info=True)

    def requeue(self, messages):
        try:
            pipe = self.broker.client.pipeline()
            for message in messages:
                stream_id = message._redis_stream_id
                pipe.xadd(self._stream_key, {"data": message.encode()})
                pipe.xack(self._stream_key, GROUP_NAME, stream_id)
                pipe.xdel(self._stream_key, stream_id)
            pipe.execute()
        except redis_mod.RedisError:
            logger.warning("Failed to requeue messages", exc_info=True)

    def close(self):
        self._closed = True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_proxy(message, entry_id):
        """Wrap a decoded Message in a MessageProxy tagged with its stream id."""
        proxy = dramatiq.MessageProxy(message)
        proxy._redis_stream_id = entry_id
        return proxy

    def _parse_entry(self, entry_id, entry_data):
        """Parse a Redis Stream entry into a :class:`dramatiq.MessageProxy`."""
        try:
            raw = entry_data.get(b"data") or entry_data.get("data")
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
            message = dramatiq.Message.decode(raw)
            return self._make_proxy(message, entry_id)
        except Exception:
            logger.warning("Failed to decode stream entry %s, discarding", entry_id, exc_info=True)
            try:
                self.broker.client.xack(self._stream_key, GROUP_NAME, entry_id)
                self.broker.client.xdel(self._stream_key, entry_id)
            except redis_mod.RedisError:
                pass
            return None

    def _task_deadline_ms(self, message):
        """Reclaim deadline (ms) for *message* — its own ``time_limit`` if it
        declares one (per-message option or per-actor), else the configured
        ``default_time_limit`` — plus the grace margin."""
        time_limit = message.options.get("time_limit")
        if time_limit is None:
            try:
                time_limit = self.broker.get_actor(message.actor_name).options.get("time_limit")
            except Exception:
                time_limit = None
        if time_limit is None:
            time_limit = self.default_time_limit
        return time_limit + self.reclaim_grace

    def _reclaim_orphans(self):
        """Reclaim messages whose owning worker appears dead.

        Crash-recovery only: a pending message is reclaimed solely once it has
        been unacked for longer than its **own** task deadline (see
        :meth:`_task_deadline_ms`). A worker still within a task's declared
        ``time_limit`` therefore never has that task stolen and re-run.

        At most ``prefetch`` messages are claimed per sweep. Claiming more than
        a worker can promptly process would hoard them in its PEL, where the
        ones it can't reach before *their* deadline would be re-stolen by other
        workers — duplicate execution and churn. ``reclaim_batch`` only bounds
        how many pending entries we *inspect* to find overdue ones.
        """
        client = self.broker.client
        try:
            pending = client.xpending_range(
                self._stream_key, GROUP_NAME, min="-", max="+",
                count=self.reclaim_batch, idle=self.reclaim_min_idle,
            )
        except redis_mod.RedisError:
            return
        if not pending:
            return

        candidates = []  # (msg_id, idle_ms)
        for entry in pending:
            # Never reclaim our own messages: this worker is alive, so its
            # in-flight tasks are not orphaned. Enforcing a task's own time
            # limit while it runs here is dramatiq's TimeLimit job, not ours —
            # re-claiming them would double-run the task.
            owner = entry.get("consumer", b"")
            if isinstance(owner, bytes):
                owner = owner.decode()
            if owner == self._consumer_name:
                continue
            mid = entry.get("message_id", b"")
            if isinstance(mid, bytes):
                mid = mid.decode()
            candidates.append((mid, entry.get("time_since_delivered", 0)))

        # Decode bodies in one pipeline to resolve each task's deadline.
        pipe = client.pipeline(transaction=False)
        for mid, _idle in candidates:
            pipe.xrange(self._stream_key, min=mid, max=mid, count=1)
        try:
            bodies = pipe.execute()
        except redis_mod.RedisError:
            return

        reclaimed = 0
        for (mid, idle), body in zip(candidates, bodies):
            if reclaimed >= self.prefetch:
                break  # don't hoard more than we can promptly process
            message = self._decode_body(body)
            if message is None:
                continue
            if idle < self._task_deadline_ms(message):
                continue
            # min_idle_time on XCLAIM re-checks the deadline server-side, so a
            # message just redelivered to (or acked by) its owner is left alone.
            try:
                claimed = client.xclaim(
                    self._stream_key, GROUP_NAME, self._consumer_name,
                    self._task_deadline_ms(message), [mid],
                )
            except redis_mod.RedisError:
                continue
            # Reuse the body we already decoded above instead of re-decoding.
            for entry_id, entry_data in claimed:
                if entry_data:
                    self._buffer.append(self._make_proxy(message, entry_id))
                    reclaimed += 1

    @staticmethod
    def _decode_body(body):
        """Decode a single ``XRANGE`` result into a Message, or None."""
        try:
            if not body:
                return None
            _eid, edata = body[0]
            raw = edata.get(b"data") or edata.get("data")
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
            return dramatiq.Message.decode(raw)
        except Exception:
            return None
