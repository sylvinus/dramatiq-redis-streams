"""Data layer for the dashboard — pure functions operating on Redis."""

import dramatiq

from ..keys import delayed_key, dlq_stream_key, stream_key, GROUP_NAME


def _decode_stream_entry(entry_id, entry_data):
    """Decode a single Redis Stream entry into a plain dict."""
    try:
        raw = entry_data.get(b"data") or entry_data.get("data")
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        msg = dramatiq.Message.decode(raw)
        return {
            "id": entry_id if isinstance(entry_id, str) else entry_id.decode(),
            "actor": msg.actor_name,
            "args": list(msg.args),
            "kwargs": msg.kwargs,
            "queue": msg.queue_name,
            "timestamp": msg.message_timestamp,
        }
    except Exception:
        eid = entry_id if isinstance(entry_id, str) else entry_id.decode()
        return {
            "id": eid,
            "actor": "<decode error>",
            "args": [],
            "kwargs": {},
            "queue": "",
            "timestamp": 0,
        }


def _discover_queues(client, namespace, declared_queues):
    """Merge declared queues with queues discovered via SCAN."""
    queues = set(declared_queues)
    prefix = f"{namespace}:stream:"
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor, match=f"{prefix}*", count=200)
        for k in keys:
            key_str = k if isinstance(k, str) else k.decode()
            queue_name = key_str[len(prefix):]
            # Skip delay queues (*.DQ suffix)
            if not queue_name.endswith(".DQ"):
                queues.add(queue_name)
        if cursor == 0:
            break
    return sorted(queues)


def get_overview(client, namespace, declared_queues=()):
    """Return an overview of all queues and delayed message count."""
    queue_names = _discover_queues(client, namespace, declared_queues)
    queues = []
    for name in queue_names:
        sk = stream_key(name, namespace)
        dk = dlq_stream_key(name, namespace)

        try:
            length = client.xlen(sk)
        except Exception:
            length = 0

        consumers = 0
        pending = 0
        entries_read = 0
        # ``lag`` (undelivered backlog) stays None unless Redis reports a usable
        # value — it goes unavailable right after deletions such as a flush.
        lag = None
        try:
            groups = client.xinfo_groups(sk)
            for g in groups:
                consumers += g.get("consumers", 0)
                pending += g.get("pending", 0)
                er = g.get("entries-read")
                if er is not None:
                    entries_read += er
                lg = g.get("lag")
                if lg is not None:
                    lag = (lag or 0) + lg
        except Exception:
            pass

        try:
            dlq_length = client.xlen(dk)
        except Exception:
            dlq_length = 0

        # ``processed`` is the cumulative number of acked messages
        # (delivered minus still-pending). Sampling it over time on the client
        # yields the queue's processing rate without any server-side state.
        queues.append({
            "name": name,
            "stream_length": length,
            "consumers": consumers,
            "pending": pending,
            "dlq_length": dlq_length,
            "lag": lag,
            "processed": max(0, entries_read - pending),
        })

    try:
        delayed_count = client.zcard(delayed_key(namespace))
    except Exception:
        delayed_count = 0

    return {"queues": queues, "delayed_count": delayed_count}


def get_queue_messages(client, namespace, queue_name, count=50):
    """Return the most recent messages in a queue's stream."""
    sk = stream_key(queue_name, namespace)
    try:
        entries = client.xrange(sk, count=count)
    except Exception:
        return []
    return [_decode_stream_entry(eid, edata) for eid, edata in entries]


def get_dlq_messages(client, namespace, queue_name, count=50):
    """Return messages in a queue's dead-letter queue."""
    dk = dlq_stream_key(queue_name, namespace)
    try:
        entries = client.xrange(dk, count=count)
    except Exception:
        return []
    return [_decode_stream_entry(eid, edata) for eid, edata in entries]


def get_delayed_messages(client, namespace, count=50):
    """Return delayed messages from the sorted set."""
    dk = delayed_key(namespace)
    try:
        entries = client.zrangebyscore(dk, "-inf", "+inf", start=0, num=count, withscores=True)
    except Exception:
        return []
    results = []
    for raw, score in entries:
        try:
            if isinstance(raw, bytes):
                raw_bytes = raw
            else:
                raw_bytes = raw.encode("utf-8")
            msg = dramatiq.Message.decode(raw_bytes)
            results.append({
                "actor": msg.actor_name,
                "queue": msg.queue_name,
                "args": list(msg.args),
                "kwargs": msg.kwargs,
                "eta_ms": score,
            })
        except Exception:
            results.append({
                "actor": "<decode error>",
                "queue": "",
                "args": [],
                "kwargs": {},
                "eta_ms": score,
            })
    return results


def delete_dlq_message(client, namespace, queue_name, stream_id):
    """Delete a single message from the DLQ. Returns True if deleted."""
    dk = dlq_stream_key(queue_name, namespace)
    try:
        return client.xdel(dk, stream_id) > 0
    except Exception:
        return False


def requeue_dlq_message(client, namespace, queue_name, stream_id):
    """Move a message from DLQ back to the main stream. Returns True on success."""
    dk = dlq_stream_key(queue_name, namespace)
    sk = stream_key(queue_name, namespace)
    try:
        entries = client.xrange(dk, min=stream_id, max=stream_id, count=1)
        if not entries:
            return False
        _eid, edata = entries[0]
        raw = edata.get(b"data") or edata.get("data")
        client.xadd(sk, {"data": raw})
        client.xdel(dk, stream_id)
        return True
    except Exception:
        return False


def purge_dlq(client, namespace, queue_name):
    """Delete all messages from a queue's DLQ. Returns count deleted."""
    dk = dlq_stream_key(queue_name, namespace)
    try:
        count = client.xlen(dk)
        client.xtrim(dk, maxlen=0)
        return count
    except Exception:
        return 0


def flush_queue(client, namespace, queue_name):
    """Flush all messages from a queue's stream and recreate the consumer group."""
    sk = stream_key(queue_name, namespace)
    client.delete(sk)
    try:
        client.xgroup_create(sk, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass


def _worker_pending_messages(client, namespace, wname, queue_info, limit):
    """Fetch up to ``limit`` pending message details for a single worker.

    The per-message detail list is purely informational, so it is capped at
    ``limit`` entries regardless of how many messages the worker actually owns.
    Message bodies (for the actor name) are decoded with a single pipeline
    round-trip instead of one ``XRANGE`` per message — critical when a worker
    holds tens of thousands of pending messages.
    """
    if limit <= 0:
        return []

    # Collect up to `limit` pending entries across the worker's queues. XPENDING
    # itself already returns idle time and delivery count cheaply.
    collected = []  # (qname, stream_key, msg_id, idle_ms, deliveries)
    for qname, qinfo in queue_info.items():
        if qinfo["pending"] == 0 or len(collected) >= limit:
            continue
        sk = stream_key(qname, namespace)
        try:
            details = client.xpending_range(
                sk, GROUP_NAME, min="-", max="+",
                count=limit - len(collected), consumername=wname,
            )
        except Exception:
            continue
        for entry in details:
            msg_id = entry.get("message_id", b"")
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode()
            collected.append((
                qname, sk, msg_id,
                entry.get("time_since_delivered", 0),
                entry.get("times_delivered", 1),
            ))
            if len(collected) >= limit:
                break

    if not collected:
        return []

    # Decode all actor names in one pipeline round-trip.
    pipe = client.pipeline(transaction=False)
    for _qname, sk, msg_id, _idle, _deliv in collected:
        pipe.xrange(sk, min=msg_id, max=msg_id, count=1)
    try:
        bodies = pipe.execute()
    except Exception:
        bodies = [None] * len(collected)

    pending_messages = []
    for (qname, _sk, msg_id, idle_ms, deliveries), body in zip(collected, bodies):
        actor = "<unknown>"
        try:
            if body:
                _, edata = body[0]
                raw = edata.get(b"data") or edata.get("data")
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                actor = dramatiq.Message.decode(raw).actor_name
        except Exception:
            pass
        pending_messages.append({
            "id": msg_id,
            "queue": qname,
            "actor": actor,
            "idle_ms": idle_ms,
            "deliveries": deliveries,
        })
    return pending_messages


def get_workers(client, namespace, declared_queues=(), pending_limit=20):
    """Return a list of workers with per-queue consumer info.

    Calls ``XINFO CONSUMERS`` on every discovered queue stream and aggregates
    the results by consumer (worker) name. Aggregate counts (``total_pending``
    and per-queue ``pending``) are always exact. The per-worker
    ``pending_messages`` detail list is capped at ``pending_limit`` entries to
    keep the endpoint responsive when workers own large backlogs.
    """
    queue_names = _discover_queues(client, namespace, declared_queues)

    # {worker_name: {"queues": {queue: {"pending": int, "idle": int}}, ...}}
    workers = {}

    for qname in queue_names:
        sk = stream_key(qname, namespace)
        try:
            consumers = client.xinfo_consumers(sk, GROUP_NAME)
        except Exception:
            continue

        for c in consumers:
            name = c.get("name", b"")
            if isinstance(name, bytes):
                name = name.decode()
            pending = c.get("pending", 0)
            idle = c.get("idle", 0)

            if name not in workers:
                workers[name] = {"queues": {}}
            workers[name]["queues"][qname] = {
                "pending": pending,
                "idle": idle,
            }

    # Build the result list and fetch pending message details per worker.
    result = []
    for wname in sorted(workers):
        wdata = workers[wname]
        queue_info = wdata["queues"]

        total_pending = sum(q["pending"] for q in queue_info.values())
        # Idle = minimum idle across queues (most-recent activity)
        min_idle = min(q["idle"] for q in queue_info.values())

        # Determine status from idle time.
        if min_idle < 60_000:          # active within the last 60 s
            status = "active"
        elif min_idle < 300_000:       # active within the last 5 min
            status = "idle"
        else:
            status = "stale"

        pending_messages = _worker_pending_messages(
            client, namespace, wname, queue_info, pending_limit,
        )

        result.append({
            "name": wname,
            "status": status,
            "idle_ms": min_idle,
            "total_pending": total_pending,
            "queues": sorted(queue_info.keys()),
            "queue_details": {
                qname: {"pending": qinfo["pending"], "idle_ms": qinfo["idle"]}
                for qname, qinfo in queue_info.items()
            },
            "pending_messages": pending_messages,
        })

    return result
