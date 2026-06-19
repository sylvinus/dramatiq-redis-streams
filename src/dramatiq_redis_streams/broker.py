import logging
import threading
import time
import uuid

import dramatiq
import redis as redis_mod

from .consumer import StreamsConsumer
from .delayed import DelayedScheduler
from .keys import GROUP_NAME, delayed_key, queues_key, stream_key

logger = logging.getLogger(__name__)


class StreamsBroker(dramatiq.Broker):
    """A Dramatiq broker that uses Redis Streams for message transport.

    Uses XREADGROUP with BLOCK for event-driven consumption (zero CPU when
    idle), Redis Streams PEL for delivery tracking, XAUTOCLAIM for dead
    consumer recovery, and a single sorted set for delayed messages.

    Requires Redis >= 7.0.

    Parameters:
        url: Redis connection URL. Ignored if ``client`` is provided.
        client: Pre-configured ``redis.Redis`` instance.
        middleware: List of Dramatiq middleware. ``None`` for defaults.
        namespace: Key prefix for all Redis keys (default ``"dramatiq"``).
    """

    def __init__(self, *, url="redis://localhost:6379/0", client=None, middleware=None, namespace="dramatiq"):
        super().__init__(middleware=middleware)
        self.namespace = namespace
        self.queues = set()  # base class uses dict; brokers use set

        if client is not None:
            self.client = client
            self._owns_client = False
        else:
            self.client = redis_mod.Redis.from_url(url)
            self._owns_client = True

        self.broker_id = uuid.uuid4().hex[:8]
        self._scheduler = None
        self._scheduler_started = False
        self._scheduler_lock = threading.Lock()
        # Queues this process has already added to the shared registry set.
        self._registered = set()

    def _ensure_group(self, key):
        """Create a consumer group on a stream, idempotently."""
        try:
            self.client.xgroup_create(key, GROUP_NAME, id="0", mkstream=True)
        except redis_mod.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _register_queue(self, queue_name, force=False):
        """Record *queue_name* in the shared registry set, once per process.

        Workers populate this set (on :meth:`consume`); the dashboard reads it
        with a single ``SMEMBERS`` to list every queue, avoiding a keyspace
        ``SCAN``. Best-effort and idempotent — :meth:`declare_queue` stays
        Redis-free, so registration happens lazily here instead.

        ``force`` re-registers even if already cached — used when a consumer
        recovers from a removed group so the queue reappears in the registry.
        """
        if queue_name in self._registered and not force:
            return
        try:
            self.client.sadd(queues_key(self.namespace), queue_name)
        except redis_mod.RedisError:
            return  # best-effort; retried on the next consume()
        self._registered.add(queue_name)

    def _start_scheduler(self):
        """Start the delayed-message scheduler if not already running."""
        with self._scheduler_lock:
            if not self._scheduler_started:
                self._scheduler = DelayedScheduler(self)
                self._scheduler.start()
                self._scheduler_started = True

    # ------------------------------------------------------------------
    # Broker interface
    # ------------------------------------------------------------------

    def declare_queue(self, queue_name):
        """Register a queue. Purely in-memory — performs no Redis I/O.

        Dramatiq calls this at import time when ``@actor`` declares an actor,
        so it must not require a live Redis connection. The Redis consumer
        group is created lazily in :meth:`consume` (i.e. at worker startup),
        which is the only place it is actually needed. Because groups are
        created at id ``"0"``, messages enqueued before any consumer starts
        are still delivered.

        Note: unlike the reference brokers, no ``.DQ`` delay queue is declared
        — see :meth:`get_declared_delay_queues`.
        """
        if queue_name not in self.queues:
            self.emit_before("declare_queue", queue_name)
            self.queues.add(queue_name)
            self.emit_after("declare_queue", queue_name)

    def enqueue(self, message, *, delay=None):
        queue_name = message.queue_name
        self.declare_queue(queue_name)
        self.emit_before("enqueue", message, delay)

        if delay is not None:
            score = time.time() * 1000 + delay
            self.client.zadd(
                delayed_key(self.namespace),
                {message.encode(): score},
            )
        else:
            self.client.xadd(
                stream_key(queue_name, self.namespace),
                {"data": message.encode()},
            )

        self.emit_after("enqueue", message, delay)
        return message

    def consume(self, queue_name, prefetch=1, timeout=30000):
        self.declare_queue(queue_name)
        # Create the consumer group lazily, here at worker startup — not in
        # declare_queue — so that importing task modules never needs Redis.
        self._ensure_group(stream_key(queue_name, self.namespace))
        # Register the queue so the dashboard can discover it without a SCAN.
        self._register_queue(queue_name)
        self._start_scheduler()
        return StreamsConsumer(
            broker=self,
            queue_name=queue_name,
            prefetch=prefetch,
            timeout=timeout,
        )

    def flush(self, queue_name):
        key = stream_key(queue_name, self.namespace)
        self.client.delete(key)
        self._ensure_group(key)

    def flush_all(self):
        for queue_name in list(self.queues):
            self.flush(queue_name)
        self.client.delete(delayed_key(self.namespace))

    def join(self, queue_name, *, interval=100, timeout=None):
        """Block until *queue_name* is empty and has no pending messages.

        Intended for use in tests.
        """
        key = stream_key(queue_name, self.namespace)
        deadline = time.time() + timeout / 1000 if timeout is not None else None

        while True:
            if deadline is not None and time.time() > deadline:
                raise dramatiq.QueueJoinTimeout(queue_name)

            try:
                length = self.client.xlen(key)
            except redis_mod.ResponseError:
                length = 0

            pending = 0
            try:
                for g in self.client.xinfo_groups(key):
                    pending += g.get("pending", 0)
            except redis_mod.ResponseError:
                pass

            if length == 0 and pending == 0:
                return

            time.sleep(interval / 1000)

    def get_declared_queues(self):
        return self.queues.copy()

    def get_declared_delay_queues(self):
        """Always empty by design.

        The reference Dramatiq brokers store delayed/retried messages on a
        per-queue ``<queue>.DQ`` queue that the Worker consumes (and a dispatch
        step moves due messages back to the canonical queue). This broker
        instead handles delays with a single ``<namespace>:delayed`` sorted set
        drained by :class:`~dramatiq_redis_streams.delayed.DelayedScheduler`,
        which re-adds due messages straight to the main stream.

        Returning an empty set therefore stops the Worker from spawning idle
        ``.DQ`` consumer threads (and creating empty ``.DQ`` streams/groups)
        that would never receive a message.
        """
        return set()

    def close(self):
        if self._scheduler is not None:
            self._scheduler.stop()
            self._scheduler.join(timeout=5)
            self._scheduler = None
            self._scheduler_started = False
        if self._owns_client:
            self.client.close()
        super().close()
