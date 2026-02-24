import logging
import threading
import time

import dramatiq
import redis as redis_mod

from .keys import delayed_key, stream_key

logger = logging.getLogger(__name__)


class DelayedScheduler(threading.Thread):
    """Daemon thread that moves delayed messages to their target streams
    when their ETA has passed.

    Multiple worker processes can each run a scheduler safely — messages
    are removed from the sorted set with ``ZREM`` which is atomic, so
    only one process will successfully claim each message.

    Parameters:
        broker: The parent :class:`StreamsBroker`.
        interval: Seconds to sleep between polls when idle (default 1.0).
        batch_size: Maximum messages to process per cycle (default 100).
    """

    def __init__(self, broker, *, interval=1.0, batch_size=100):
        super().__init__(daemon=True, name="DelayedScheduler")
        self.broker = broker
        self.interval = interval
        self.batch_size = batch_size
        self._stop_event = threading.Event()

    def run(self):
        logger.debug("Delayed scheduler started (interval=%.1fs)", self.interval)
        while not self._stop_event.is_set():
            try:
                moved = self._process()
                if moved == 0:
                    self._stop_event.wait(self.interval)
                # If messages were moved, loop immediately to check for more.
            except redis_mod.ConnectionError:
                logger.warning("Redis connection lost in delayed scheduler, retrying")
                self._stop_event.wait(self.interval)
            except Exception:
                logger.warning("Delayed scheduler error", exc_info=True)
                self._stop_event.wait(self.interval)
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
