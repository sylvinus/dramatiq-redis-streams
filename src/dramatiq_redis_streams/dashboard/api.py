"""Data layer for the dashboard — pure functions operating on Redis."""

import dramatiq

from ..keys import delayed_key, dlq_stream_key, queues_key, stream_key, GROUP_NAME


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
    """Return all known queues: the declared set unioned with the shared
    registry (``<namespace>:queues``).

    The registry is populated by workers (on consume), so a single ``SMEMBERS``
    lists every queue — no keyspace ``SCAN``.
    """
    queues = set(declared_queues)
    try:
        members = client.smembers(queues_key(namespace))
    except Exception:
        members = []
    for m in members:
        name = m.decode() if isinstance(m, bytes) else m
        # Defensive: never surface delay-queue names.
        if not name.endswith(".DQ"):
            queues.add(name)
    return sorted(queues)


def get_overview(client, namespace, declared_queues=()):
    """Return an overview of all queues and delayed message count.

    All per-queue reads (``XLEN`` main, ``XINFO GROUPS``, ``XLEN`` dlq) plus the
    delayed ``ZCARD`` are issued in a single pipeline — one round-trip instead
    of ~3 per queue. Per-command errors (e.g. a missing group) surface as
    exceptions in the result list and degrade to zero, not a failed request.
    """
    queue_names = _discover_queues(client, namespace, declared_queues)

    pipe = client.pipeline(transaction=False)
    for name in queue_names:
        pipe.xlen(stream_key(name, namespace))
        pipe.xinfo_groups(stream_key(name, namespace))
        pipe.xlen(dlq_stream_key(name, namespace))
    pipe.zcard(delayed_key(namespace))
    try:
        results = pipe.execute(raise_on_error=False)
    except Exception:
        results = []

    def at(i):
        return results[i] if 0 <= i < len(results) else None

    def num(v):
        return v if isinstance(v, int) else 0

    queues = []
    for i, name in enumerate(queue_names):
        groups = at(i * 3 + 1)
        consumers = pending = entries_read = 0
        # ``lag`` (undelivered backlog) stays None unless Redis reports a usable
        # value — it goes unavailable right after deletions such as a flush.
        lag = None
        if isinstance(groups, list):
            for g in groups:
                consumers += g.get("consumers", 0)
                pending += g.get("pending", 0)
                er = g.get("entries-read")
                if er is not None:
                    entries_read += er
                lg = g.get("lag")
                if lg is not None:
                    lag = (lag or 0) + lg

        # ``processed`` is the cumulative number of acked messages (delivered
        # minus still-pending). Sampling it over time on the client yields the
        # queue's processing rate without any server-side state.
        queues.append({
            "name": name,
            "stream_length": num(at(i * 3)),
            "consumers": consumers,
            "pending": pending,
            "dlq_length": num(at(i * 3 + 2)),
            "lag": lag,
            "processed": max(0, entries_read - pending),
        })

    delayed_count = num(at(len(queue_names) * 3))
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


def requeue_all_dlq(client, namespace, queue_name, batch=500):
    """Move every message from a queue's DLQ back to the main stream.

    Returns the number of messages requeued. Messages are moved oldest-first so
    FIFO order is preserved, in batches to bound memory on large DLQs. Each
    batch is read then re-added/deleted; because entries are deleted as they are
    processed, the next read returns the remaining ones.
    """
    dk = dlq_stream_key(queue_name, namespace)
    sk = stream_key(queue_name, namespace)
    moved = 0
    while True:
        try:
            entries = client.xrange(dk, count=batch)
        except Exception:
            break
        if not entries:
            break
        pipe = client.pipeline(transaction=False)
        n = 0
        for eid, edata in entries:
            raw = edata.get(b"data") or edata.get("data")
            if raw is not None:
                pipe.xadd(sk, {"data": raw})
                n += 1
            # Delete every entry we read (including corrupt ones) so the DLQ
            # strictly shrinks and we never loop on an un-requeueable message.
            pipe.xdel(dk, eid)
        try:
            pipe.execute()
        except Exception:
            break
        moved += n
        if len(entries) < batch:
            break
    return moved


def purge_dlq(client, namespace, queue_name):
    """Delete all messages from a queue's DLQ. Returns count deleted."""
    dk = dlq_stream_key(queue_name, namespace)
    try:
        count = client.xlen(dk)
        client.xtrim(dk, maxlen=0)
        return count
    except Exception:
        return 0


def remove_queue(client, namespace, queue_name):
    """Remove a queue entirely: registry entry, main stream, and DLQ stream.

    Intended for cleaning up queues that are no longer used; the dashboard only
    offers it for empty queues. If a worker is still consuming the queue it will
    transparently recreate and re-register it (see the consumer's ``NOGROUP``
    recovery), so an in-use queue simply reappears. Returns True on success.
    """
    try:
        client.srem(queues_key(namespace), queue_name)
        client.delete(stream_key(queue_name, namespace))
        client.delete(dlq_stream_key(queue_name, namespace))
        return True
    except Exception:
        return False


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
