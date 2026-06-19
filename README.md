# dramatiq-redis-streams

A [Redis Streams](https://redis.io/docs/data-types/streams-tutorial/) broker for [Dramatiq](https://dramatiq.io/).

Replaces Dramatiq's built-in `RedisBroker` (which polls with Lua scripts) with an event-driven implementation using `XREADGROUP BLOCK` — zero CPU when idle, deterministic failure recovery, and fewer Redis connections.

## Why

| Aspect | Built-in RedisBroker | This broker |
|---|---|---|
| Consumption | Lua LPOP + exponential backoff | `XREADGROUP BLOCK` (event-driven) |
| Delivery tracking | Custom ack set in Lua | Redis Streams PEL (built-in) |
| Dead worker recovery | Probabilistic (0.1% per poll) | Per-task deadline (never steals a task within its `time_limit`) |
| Delayed messages | Per-queue `.DQ` list + Lua poll | Single sorted set + 1 scheduler thread |
| Dead letter | Custom sorted set + Lua | DLQ stream per queue |
| Idle Redis ops/sec | ~48 EVALSHA/sec | ~1/sec (scheduler only) |

Requires **Redis >= 7.0**.

## Installation

```bash
pip install dramatiq-redis-streams
```

Or from source:

```bash
pip install git+https://github.com/sylvinus/dramatiq-redis-streams.git
```

## Quick Start

```python
import dramatiq
from dramatiq_redis_streams import StreamsBroker

broker = StreamsBroker(url="redis://localhost:6379/0")
dramatiq.set_broker(broker)

@dramatiq.actor
def add(x, y):
    print(f"Result: {x + y}")

# Send a message
add.send(1, 2)

# Send with a delay (milliseconds)
add.send_with_options(args=(3, 4), delay=5000)
```

Run workers with the standard Dramatiq CLI:

```bash
dramatiq my_module
```

## Configuration

```python
StreamsBroker(
    url="redis://localhost:6379/0",  # Redis URL (ignored if client is set)
    client=None,                      # Pre-configured redis.Redis instance
    middleware=None,                   # Dramatiq middleware list (None = defaults)
    namespace="dramatiq",             # Key prefix for all Redis keys
    default_time_limit=60000,         # Timeout for tasks with no time_limit
    reclaim_grace=10000,              # Extra time past a task's deadline before reclaim
    reclaim_interval=30000,           # Time between orphan-recovery sweeps
)
```

All broker time values are in **milliseconds**, matching dramatiq (`time_limit`, `delay`, etc.).

### Task timeouts and recovery

A message is only recovered from a worker once it has been unacked for longer
than **that task's own deadline** — its `time_limit` (per actor or per message),
plus `reclaim_grace`. A worker legitimately running a task within its declared
`time_limit` is therefore never robbed of it.

`default_time_limit` (60s) applies to tasks that declare no `time_limit`. It is a
single value with two aligned effects: dramatiq's `TimeLimit` middleware raises
`TimeLimitExceeded` **in the worker** at that point, and the broker treats a
message unacked past it as orphaned. Set a per-task limit to move both together:

```python
@dramatiq.actor(time_limit=300000)   # 5 minutes; abort AND reclaim deadline
def slow_task(): ...
```

When you supply your own `middleware`, the reclaim deadline follows your
`TimeLimit` (if any) so the two can't diverge; if it contains no `TimeLimit`,
`default_time_limit` is used as the reclaim fallback only.

### Redis Data Model

| Key | Type | Purpose |
|---|---|---|
| `{namespace}:stream:{queue}` | Stream | Main message queue |
| `{namespace}:delayed` | Sorted Set | Delayed messages (score = ETA in ms) |
| `{namespace}:dlq:{queue}` | Stream | Dead-letter queue per queue |
| `{namespace}:queues` | Set | Registry of known queues (for dashboard discovery) |

Consumer group `workers` on each stream, consumer name `worker-{broker_id}` per process.

The queue registry is populated by **workers** (on `consume`) and read by the
dashboard, so a queue appears in the dashboard once a worker has started
consuming it — not merely when messages are enqueued to it.

## Dashboard

A built-in web dashboard for monitoring queues, inspecting messages, and managing dead-letter queues. Zero additional dependencies. It shows per-queue **backlog** (undelivered) and **throughput** (acked/sec, 1-min average), per-worker **reserved** messages, and offers **Flush**/**Remove**, plus DLQ **Requeue All**/**Purge All**.

> **⚠️ Security.** The dashboard exposes **destructive, unauthenticated** endpoints (flush, remove, purge, requeue) and serves task payloads. It performs **no authentication or CSRF protection** itself — the Django helper is even `csrf_exempt`. **You must put it behind authentication and network restrictions** (e.g. an authenticated reverse proxy, IP allowlist, or your framework's auth). Never expose it publicly.

![Overview](docs/screenshots/overview.png)

### Standalone

```bash
python -m dramatiq_redis_streams.dashboard --redis-url redis://localhost:6379/0 --port 8080
```

### Django Integration

```python
# urls.py
from dramatiq_redis_streams.dashboard import get_urlpatterns
from myapp import broker

urlpatterns += get_urlpatterns(broker, prefix="dramatiq/")
```

### Programmatic (WSGI)

```python
from dramatiq_redis_streams.dashboard import DashboardApp

app = DashboardApp(broker, prefix="/dashboard")
# Mount with any WSGI server (gunicorn, uwsgi, etc.)
```

### Views

**Queue detail** — stream ID, actor, args/kwargs, and timestamp for each message:

![Queue detail](docs/screenshots/queue-detail.png)

**Dead Letter Queue** — failed messages with Requeue and Delete actions, plus bulk Purge:

![DLQ detail](docs/screenshots/dlq-detail.png)

**Workers** — live worker processes with status, queues, idle time, and pending message details:

![Workers](docs/screenshots/workers.png)

**Delayed messages** — scheduled messages with their ETAs:

![Delayed messages](docs/screenshots/delayed.png)

## Development

All development uses Docker — no host Python required.

```bash
# Run tests
make test

# Run tests with verbose output
make test-verbose

# Open a shell in the container
make shell

# Lint
make lint

# Clean up
make clean
```

### Running a specific test

```bash
docker compose run --rm test pytest tests/test_broker.py -v
```

## Architecture

### Consumer (`StreamsConsumer`)

Each consumer thread calls `XREADGROUP GROUP workers {id} BLOCK {timeout} COUNT {prefetch} STREAMS {key} >`. This blocks efficiently on the Redis server — the thread uses zero CPU while waiting.

Every `reclaim_interval` ms (default 30 000), the consumer sweeps the group's pending entry list (`XPENDING`) for messages owned by **other** workers that have been unacked longer than their own task deadline (`time_limit` + `reclaim_grace`), and `XCLAIM`s those — recovering work from dead workers without ever stealing a task a live worker is still within-deadline on. A worker never reclaims its own messages. Separately, the scheduler thread reaps fully-drained consumer records that have been idle beyond an hour via `XGROUP DELCONSUMER`.

### Message Lifecycle

- **ack**: `XACK` + `XDEL` (removes from PEL and frees stream memory)
- **nack**: `XADD` to DLQ stream, then `XACK` + `XDEL` from main stream
- **requeue**: `XADD` new entry + `XACK` + `XDEL` old entry (atomic via pipeline)

### Delayed Messages (`DelayedScheduler`)

One daemon thread per worker process polls the `{namespace}:delayed` sorted set every second. Due messages (score ≤ now) are atomically removed with `ZREM` and added to their target stream with `XADD`. Multiple processes can safely run schedulers concurrently.

## License

MIT
