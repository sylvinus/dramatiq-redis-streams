import logging
import time

import dramatiq
import redis as redis_mod

from .keys import GROUP_NAME, consumer_name, dlq_stream_key, stream_key

logger = logging.getLogger(__name__)


class StreamsConsumer(dramatiq.Consumer):
    """A Dramatiq consumer backed by Redis Streams.

    Uses ``XREADGROUP`` with ``BLOCK`` for efficient, event-driven message
    consumption.  Orphaned messages from dead consumers are periodically
    recovered via ``XAUTOCLAIM``.

    Parameters:
        broker: The parent :class:`StreamsBroker`.
        queue_name: Queue to consume from.
        prefetch: Max messages to fetch per ``XREADGROUP`` call.
        timeout: Block timeout in milliseconds.
        autoclaim_interval: Seconds between XAUTOCLAIM sweeps (default 60).
        min_idle_time: Minimum idle time in ms before a message can be
            autoclaimed (default 60 000).
    """

    def __init__(
        self,
        *,
        broker,
        queue_name,
        prefetch=1,
        timeout=30000,
        autoclaim_interval=60,
        min_idle_time=60000,
    ):
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout = timeout
        self.autoclaim_interval = autoclaim_interval
        self.min_idle_time = min_idle_time

        self._buffer = []
        self._last_autoclaim = 0.0
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

        # 2. Periodic XAUTOCLAIM to recover orphaned messages.
        now = time.monotonic()
        if now - self._last_autoclaim >= self.autoclaim_interval:
            self._autoclaim()
            self._last_autoclaim = now
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

    def _parse_entry(self, entry_id, entry_data):
        """Parse a Redis Stream entry into a :class:`dramatiq.MessageProxy`."""
        try:
            raw = entry_data.get(b"data") or entry_data.get("data")
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
            message = dramatiq.Message.decode(raw)
            proxy = dramatiq.MessageProxy(message)
            proxy._redis_stream_id = entry_id
            return proxy
        except Exception:
            logger.warning("Failed to decode stream entry %s, discarding", entry_id, exc_info=True)
            try:
                self.broker.client.xack(self._stream_key, GROUP_NAME, entry_id)
                self.broker.client.xdel(self._stream_key, entry_id)
            except redis_mod.RedisError:
                pass
            return None

    def _autoclaim(self):
        """Claim orphaned messages from dead consumers via XAUTOCLAIM."""
        try:
            result = self.broker.client.xautoclaim(
                self._stream_key,
                GROUP_NAME,
                self._consumer_name,
                min_idle_time=self.min_idle_time,
                start_id="0-0",
                count=self.prefetch,
            )
        except (redis_mod.ResponseError, redis_mod.ConnectionError):
            return

        if not result or not result[1]:
            return

        for entry_id, entry_data in result[1]:
            if entry_data:  # skip tombstones (deleted entries)
                proxy = self._parse_entry(entry_id, entry_data)
                if proxy is not None:
                    self._buffer.append(proxy)
