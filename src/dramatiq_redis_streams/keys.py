"""Redis key naming helpers for the Streams broker."""

GROUP_NAME = "workers"

# Sentinel consumer that owns messages abandoned at shutdown. Using a name no
# real worker has means any worker — including a same-named restart — reclaims
# them (a consumer never reclaims its *own* pending). See StreamsConsumer.close.
ABANDONED_CONSUMER = "abandoned"


def stream_key(queue_name, namespace="dramatiq"):
    """Return the Redis Stream key for a queue."""
    return f"{namespace}:stream:{queue_name}"


def delayed_key(namespace="dramatiq"):
    """Return the Redis Sorted Set key for delayed messages."""
    return f"{namespace}:delayed"


def queues_key(namespace="dramatiq"):
    """Return the Redis Set key listing all known queue names.

    Populated lazily by the broker (on enqueue/consume) so the dashboard can
    list every queue with a single ``SMEMBERS`` instead of a keyspace ``SCAN``.
    """
    return f"{namespace}:queues"


def dlq_stream_key(queue_name, namespace="dramatiq"):
    """Return the Redis Stream key for a queue's dead-letter queue."""
    return f"{namespace}:dlq:{queue_name}"


def dlq_expiry_key(queue_name, namespace="dramatiq"):
    """Return the Sorted Set key indexing DLQ entries by expiry time.

    Maps DLQ stream ID -> expiry timestamp (ms) for messages with a
    ``dead_message_ttl``. The scheduler drops entries whose time has passed.
    """
    return f"{namespace}:dlq_expiry:{queue_name}"


def consumer_name(broker_id):
    """Return the consumer name for a given broker ID."""
    return f"worker-{broker_id}"
