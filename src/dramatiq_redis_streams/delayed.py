import logging
import threading
import time

import dramatiq
import redis as redis_mod

from .keys import (
    ABANDONED_CONSUMER,
    GROUP_NAME,
    delayed_key,
    dlq_expiry_key,
    dlq_stream_key,
    stream_key,
)

logger = logging.getLogger(__name__)


class DelayedScheduler(threading.Thread):
    """Daemon thread that moves delayed messages to their target streams
    when their ETA has passed.

    Multiple worker processes can each run a scheduler safely — messages
    are removed from the sorted set with ``ZREM`` which is atomic, so
    only one process will successfully claim each message.

    All time values are in milliseconds, matching dramatiq.

    Parameters:
        broker: The parent :class:`StreamsBroker`.
        interval: Milliseconds to sleep between polls when idle (default 1 000).
        batch_size: Maximum messages to process per cycle (default 100).
        reap_interval: Milliseconds between stale-consumer sweeps (default 60 000).
        reap_min_idle: Minimum consumer idle time, in milliseconds, before a
            drained consumer is removed (default 1 hour).
        dlq_expire_batch: Max expired dead-letters deleted per queue per sweep.
    """

    def __init__(
        self,
        broker,
        *,
        interval=1000,
        batch_size=100,
        reap_interval=60000,
        reap_min_idle=3_600_000,
        dlq_expire_batch=1000,
    ):
        super().__init__(daemon=True, name="DelayedScheduler")
        self.broker = broker
        self.interval = interval
        self.batch_size = batch_size
        self.reap_interval = reap_interval
        self.reap_min_idle = reap_min_idle
        self.dlq_expire_batch = dlq_expire_batch
        self._stop_event = threading.Event()
        self._last_reap = 0.0

    def run(self):
        logger.debug("Delayed scheduler started (interval=%dms)", self.interval)
        while not self._stop_event.is_set():
            try:
                moved = self._process()
                self._maybe_reap()
                if moved == 0:
                    self._stop_event.wait(self.interval / 1000)
                # If messages were moved, loop immediately to check for more.
            except redis_mod.ConnectionError:
                logger.warning("Redis connection lost in delayed scheduler, retrying")
                self._stop_event.wait(self.interval / 1000)
            except Exception:
                logger.warning("Delayed scheduler error", exc_info=True)
                self._stop_event.wait(self.interval / 1000)
        logger.debug("Delayed scheduler stopped")

    def stop(self):
        """Signal the scheduler to stop."""
        self._stop_event.set()

    def _process(self):
        """Move due delayed messages to their target streams.

        Returns the number of messages moved.
        """
        now = time.time() * 1000  # milliseconds
        dkey = delayed_key(self.broker.namespace)

        entries = self.broker.client.zrangebyscore(
            dkey, "-inf", now, start=0, num=self.batch_size
        )

        moved = 0
        for entry in entries:
            # Atomic remove — only one process wins the race.
            if self.broker.client.zrem(dkey, entry):
                try:
                    message = dramatiq.Message.decode(entry)
                    target = stream_key(message.queue_name, self.broker.namespace)
                    self.broker.client.xadd(target, {"data": entry})
                    moved += 1
                except Exception:
                    logger.warning("Failed to move delayed message to stream", exc_info=True)
                    # Re-add with score=0 so it's retried on the next cycle
                    # rather than silently lost.
                    try:
                        self.broker.client.zadd(dkey, {entry: 0})
                    except Exception:
                        logger.error("Delayed message lost — ZREM succeeded but XADD and re-ZADD both failed", exc_info=True)
        return moved

    def _maybe_reap(self):
        """Run the periodic maintenance sweeps if ``reap_interval`` elapsed."""
        now = time.monotonic()
        if now - self._last_reap < self.reap_interval / 1000:
            return
        self._last_reap = now
        for sweep in (self._reap_consumers, self._expire_dead_letters):
            try:
                sweep()
            except redis_mod.ConnectionError:
                logger.warning("Redis connection lost during periodic sweep")
            except Exception:
                logger.warning("Periodic sweep error", exc_info=True)

    def _expire_dead_letters(self):
        """Delete dead-lettered messages whose ``dead_message_ttl`` has elapsed.

        Cheap and O(expired): an expiry sorted set is consulted by score, so
        messages kept forever (no TTL) are never even looked at.
        """
        client = self.broker.client
        namespace = self.broker.namespace
        now = int(time.time() * 1000)
        for queue_name in self.broker.get_declared_queues():
            ek = dlq_expiry_key(queue_name, namespace)
            try:
                expired = client.zrangebyscore(ek, "-inf", now, start=0, num=self.dlq_expire_batch)
            except redis_mod.ResponseError:
                continue
            if not expired:
                continue
            ids = [e.decode() if isinstance(e, bytes) else e for e in expired]
            dk = dlq_stream_key(queue_name, namespace)
            try:
                client.xdel(dk, *ids)
                client.zrem(ek, *ids)
            except redis_mod.ResponseError:
                pass

    def _reap_consumers(self):
        """Remove stale, fully-drained consumers from each stream's group.

        Only the broker's **declared** queues are swept (one ``XINFO CONSUMERS``
        per queue) — never a keyspace ``SCAN``, which would walk every key in a
        possibly-shared Redis on each sweep. A worker process declares the
        queues it consumes, so a live worker reaps the dead consumers on exactly
        those streams; queues with no live worker have no scheduler to reap them
        and their idle consumers are inert anyway.

        A consumer is deleted only when it owns **no** pending messages and has
        been idle longer than ``reap_min_idle`` — by which point the
        orphan-recovery sweep (``StreamsConsumer._reclaim_orphans``) has long
        since recovered any work it held. ``XGROUP DELCONSUMER`` is idempotent,
        so multiple scheduler processes racing to reap the same consumer is
        harmless. (A live but quiet worker that gets reaped simply re-creates
        its consumer on the next ``XREADGROUP``.)

        Returns the number of consumers reaped.
        """
        client = self.broker.client
        namespace = self.broker.namespace
        reaped = 0
        for queue_name in self.broker.get_declared_queues():
            sk = stream_key(queue_name, namespace)
            try:
                consumers = client.xinfo_consumers(sk, GROUP_NAME)
            except redis_mod.ResponseError:
                continue
            for c in consumers:
                if c.get("pending", 0) != 0:
                    continue
                name = c.get("name", b"")
                if isinstance(name, bytes):
                    name = name.decode()
                # Real workers are kept until idle a long time (a quiet worker
                # isn't dead). The abandoned sentinel, once drained, is reaped
                # immediately — it's recreated on the next abandon and shouldn't
                # linger as a phantom consumer.
                if name != ABANDONED_CONSUMER and c.get("idle", 0) < self.reap_min_idle:
                    continue
                try:
                    client.xgroup_delconsumer(sk, GROUP_NAME, name)
                    reaped += 1
                except redis_mod.ResponseError:
                    pass
        if reaped:
            logger.debug("Reaped %d stale consumer(s)", reaped)
        return reaped
