"""Redis key naming helpers for the Streams broker."""

GROUP_NAME = "workers"


def stream_key(queue_name, namespace="dramatiq"):
    """Return the Redis Stream key for a queue."""
    return f"{namespace}:stream:{queue_name}"


def delayed_key(namespace="dramatiq"):
    """Return the Redis Sorted Set key for delayed messages."""
    return f"{namespace}:delayed"


def dlq_stream_key(queue_name, namespace="dramatiq"):
    """Return the Redis Stream key for a queue's dead-letter queue."""
    return f"{namespace}:dlq:{queue_name}"


def consumer_name(broker_id):
    """Return the consumer name for a given broker ID."""
    return f"worker-{broker_id}"
