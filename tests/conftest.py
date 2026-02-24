import os
import time

import dramatiq
import pytest
import redis

from dramatiq_redis_streams import StreamsBroker

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

def make_message(queue="test-queue", actor="test-actor", args=(), kwargs=None):
    """Create a dramatiq Message for testing."""
    return dramatiq.Message(
        queue_name=queue,
        actor_name=actor,
        args=args,
        kwargs=kwargs or {},
        options={},
    )


def wait_for(predicate, timeout=3, step=0.05):
    """Spin until *predicate()* is truthy or *timeout* seconds elapse."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def redis_client():
    """Raw Redis client (bytes mode) — shared with the broker under test."""
    client = redis.Redis.from_url(REDIS_URL)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def broker(redis_client):
    """A StreamsBroker wired to the test Redis instance."""
    b = StreamsBroker(client=redis_client, middleware=[])
    dramatiq.set_broker(b)
    yield b
    b.close()
