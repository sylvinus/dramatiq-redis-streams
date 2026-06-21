import logging
import threading
import time

import dramatiq
import redis as redis_mod

from .keys import (
    ABANDONED_CONSUMER,
    GROUP_NAME,
    consumer_name,
    dlq_expiry_key,
    dlq_stream_key,
    stream_key,
)

logger = logging.getLogger(__name__)

# Fallback wait (seconds) when at the prefetch limit, if no ack wakes us sooner.
_AT_CAPACITY_WAIT = 1.0

# Idle time (ms) stamped on messages abandoned at shutdown — far beyond any
# real task deadline, so the next reclaim sweep treats them as orphaned at once.
_ABANDON_IDLE_MS = 10 ** 12  # ~31 years

# Hard cap on the failure traceback stored on a dead-lettered message, so a DLQ
# entry can't balloon. dramatiq already limits its traceback to 30 frames.
_MAX_DLQ_ERROR = 8192


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
            message declares no ``time_limit`` (default 600 000, matching
            dramatiq's ``TimeLimit``; the broker passes the configured limit).
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
        default_time_limit=600000,
        reclaim_grace=10000,
        reclaim_min_idle=1000,
        reclaim_batch=100,
        dead_message_ttl=None,
    ):
        self.broker = broker
        self.queue_name = queue_name
        # A non-positive prefetch would leave `room` permanently 0 and the
        # consumer would never fetch anything — clamp it so that can't happen.
        self.prefetch = max(1, prefetch)
        self.timeout = timeout
        self.reclaim_interval = reclaim_interval
        self.default_time_limit = default_time_limit
        self.reclaim_grace = reclaim_grace
        self.reclaim_min_idle = reclaim_min_idle
        self.reclaim_batch = reclaim_batch
        self.dead_message_ttl = dead_message_ttl

        self._buffer = []
        self._unacked_lock = threading.Lock()
        # Signalled by ack/nack/requeue so a capacity-blocked reader wakes
        # immediately when a slot frees, instead of polling.
        self._slot_freed = threading.Event()
        self._last_reclaim = 0.0
        self._closed = False
        self._consumer_name = consumer_name(broker.broker_id)
        self._stream_key = stream_key(queue_name, broker.namespace)
        # Messages delivered to us (in our PEL) but not yet acked/nacked. The
        # prefetch limit is enforced against this so one worker can't reserve a
        # whole backlog. Mutated from worker threads (ack/nack) too, hence lock.
        # Seed from the existing PEL: dramatiq routes acks to the *current*
        # consumer instance, so a restart inherits its predecessor's pending
        # messages under the same consumer name — count them or we'd over-reserve.
        self._unacked = self._pending_count()

    def _pending_count(self):
        """Messages already pending for this consumer name in Redis (its PEL)."""
        try:
            consumers = self.broker.client.xinfo_consumers(self._stream_key, GROUP_NAME)
        except redis_mod.RedisError:
            return 0
        for c in consumers:
            name = c.get("name", b"")
            if isinstance(name, bytes):
                name = name.decode()
            if name == self._consumer_name:
                return c.get("pending", 0)
        return 0

    def _track(self, delta):
        """Adjust the delivered-but-unacked counter (thread-safe)."""
        with self._unacked_lock:
            self._unacked = max(0, self._unacked + delta)

    def _release(self, n):
        """Mark *n* messages done and wake a capacity-blocked reader."""
        self._track(-n)
        self._slot_freed.set()

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

        # 2. Enforce the prefetch limit on outstanding (unacked) messages.
        #    dramatiq's work queue is unbounded and never back-pressures, so a
        #    consumer that kept reading would siphon the whole backlog into its
        #    own PEL and starve other workers. Never hold more than `prefetch`.
        room = self.prefetch - self._unacked
        if room <= 0:
            # At capacity: block until a slot frees on ack (event-driven, not a
            # busy poll), then let dramatiq call us again. The cap bounds how
            # long we wait if every thread is stuck, keeping shutdown responsive.
            self._slot_freed.wait(_AT_CAPACITY_WAIT)
            self._slot_freed.clear()
            return None

        # 3. Periodic sweep to recover messages orphaned by dead workers,
        #    bounded by the room left under the prefetch limit.
        now = time.monotonic()
        if now - self._last_reclaim >= self.reclaim_interval / 1000:
            self._reclaim_orphans(room)
            self._last_reclaim = now
            if self._buffer:
                return self._buffer.pop(0)

        # 4. XREADGROUP BLOCK — efficient server-side wait, up to `room`.
        try:
            results = self.broker.client.xreadgroup(
                GROUP_NAME,
                self._consumer_name,
                {self._stream_key: ">"},
                count=room,
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
                        self._track(1)

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
        self._release(1)

    def nack(self, message):
        self._record_failure(message)
        try:
            stream_id = message._redis_stream_id
            dlq_key = dlq_stream_key(self.queue_name, self.broker.namespace)

            pipe = self.broker.client.pipeline()
            pipe.xadd(dlq_key, {"data": message.encode()})
            pipe.xack(self._stream_key, GROUP_NAME, stream_id)
            pipe.xdel(self._stream_key, stream_id)
            dlq_id = pipe.execute()[0]
            self._schedule_dlq_expiry(message, dlq_id)
        except redis_mod.RedisError:
            logger.warning("Failed to nack message %s", message.message_id, exc_info=True)
        self._release(1)

    def _dead_message_ttl_ms(self, message):
        """Resolve the dead-letter lifetime (ms) for *message*: its own
        ``dead_message_ttl`` option, else the actor's, else the broker default."""
        ttl = message.options.get("dead_message_ttl")
        if ttl is None:
            try:
                ttl = self.broker.get_actor(message.actor_name).options.get("dead_message_ttl")
            except Exception:
                ttl = None
        if ttl is None:
            ttl = self.dead_message_ttl
        return ttl

    def _schedule_dlq_expiry(self, message, dlq_id):
        """Index a dead-lettered message for auto-deletion if it has a TTL."""
        ttl = self._dead_message_ttl_ms(message)
        if not ttl or ttl <= 0:
            return  # kept until purged
        if isinstance(dlq_id, bytes):
            dlq_id = dlq_id.decode()
        try:
            self.broker.client.zadd(
                dlq_expiry_key(self.queue_name, self.broker.namespace),
                {dlq_id: time.time() * 1000 + ttl},
            )
        except redis_mod.RedisError:
            pass

    @staticmethod
    def _record_failure(message):
        """Make sure a bounded failure reason rides along to the DLQ.

        dramatiq's Retries middleware stores a (30-frame) traceback in
        ``options["traceback"]`` for normal failures; fill it in from the stuffed
        exception for the paths that don't (ActorNotFound, expected ``throws``),
        and hard-cap the size so a DLQ entry can't balloon.
        """
        try:
            tb = message.options.get("traceback")
            if not tb:
                exc = getattr(message, "_exception", None)
                tb = f"{type(exc).__name__}: {exc}" if exc is not None else None
            if tb and len(tb) > _MAX_DLQ_ERROR:
                # Keep the tail — the exception and innermost frames matter most.
                tb = "...(truncated)\n" + tb[-_MAX_DLQ_ERROR:]
            if tb:
                message.options["traceback"] = tb
        except Exception:
            pass

    def requeue(self, messages):
        count = 0
        try:
            pipe = self.broker.client.pipeline()
            for message in messages:
                stream_id = message._redis_stream_id
                pipe.xadd(self._stream_key, {"data": message.encode()})
                pipe.xack(self._stream_key, GROUP_NAME, stream_id)
                pipe.xdel(self._stream_key, stream_id)
                count += 1
            pipe.execute()
        except redis_mod.RedisError:
            logger.warning("Failed to requeue messages", exc_info=True)
        self._release(count)

    def close(self):
        self._closed = True
        # Hand back anything we prefetched but never started so other workers
        # pick it up immediately — and *ahead of* the backlog, since reclaim
        # runs before XREADGROUP. Rather than re-enqueue (append-only → tail),
        # stamp the messages as maximally idle in place with XCLAIM, so the next
        # reclaim sweep treats them as orphaned and claims them at once. Keeps
        # their position and stream IDs, and needs no extra bookkeeping.
        # dramatiq has already stopped and joined the consumer thread, so the
        # buffer is ours to drain.
        buffered, self._buffer = self._buffer, []
        if not buffered:
            return
        try:
            # Claim to the ABANDONED sentinel (not ourselves): dramatiq also
            # calls close() on the error path and then recreates a consumer with
            # the *same* name, which would skip its own pending — owning them by
            # a name no worker uses keeps them reclaimable by anyone.
            self.broker.client.xclaim(
                self._stream_key, GROUP_NAME, ABANDONED_CONSUMER,
                0, [m._redis_stream_id for m in buffered],
                idle=_ABANDON_IDLE_MS, justid=True,
            )
        except redis_mod.RedisError:
            logger.warning("Failed to abandon buffered messages on close", exc_info=True)

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
                pipe = self.broker.client.pipeline()
                pipe.xack(self._stream_key, GROUP_NAME, entry_id)
                pipe.xdel(self._stream_key, entry_id)
                pipe.execute()
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

    def _reclaim_orphans(self, limit=None):
        """Reclaim messages whose owning worker appears dead.

        Crash-recovery only: a pending message is reclaimed solely once it has
        been unacked for longer than its **own** task deadline (see
        :meth:`_task_deadline_ms`). A worker still within a task's declared
        ``time_limit`` therefore never has that task stolen and re-run.

        At most ``limit`` messages are claimed per sweep (defaults to
        ``prefetch``). Claiming more than a worker can promptly process would
        hoard them in its PEL, where the ones it can't reach before *their*
        deadline would be re-stolen by other workers — duplicate execution and
        churn. ``reclaim_batch`` only bounds how many pending entries we
        *inspect* to find overdue ones.
        """
        if limit is None:
            limit = self.prefetch
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
            if not body:
                # Pending entry whose payload was deleted from the stream (e.g.
                # trimmed): XCLAIM+JUSTID purges the dangling PEL entry
                # (Redis 7.0+), so it can't wedge the owning consumer forever.
                try:
                    client.xclaim(self._stream_key, GROUP_NAME,
                                  self._consumer_name, 0, [mid], justid=True)
                except redis_mod.RedisError:
                    pass
                continue
            if reclaimed >= limit:
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
                    self._track(1)
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
