# dramatiq-redis-streams

A [Redis Streams](https://redis.io/docs/data-types/streams-tutorial/) broker for [Dramatiq](https://dramatiq.io/).

Replaces Dramatiq's built-in `RedisBroker` (which polls with Lua scripts) with an event-driven implementation using `XREADGROUP BLOCK` — zero CPU when idle, deterministic failure recovery, and fewer Redis connections.

## Why

| Aspect | Built-in RedisBroker | This broker |
|---|---|---|
| Consumption | Lua LPOP + exponential backoff | `XREADGROUP BLOCK` (event-driven) |
| Delivery tracking | Custom ack set in Lua | Redis Streams PEL (built-in) |
| Dead consumer recovery | Probabilistic (0.1% per poll) | Deterministic (`XAUTOCLAIM` every 60s) |
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
)
```

### Redis Data Model

| Key | Type | Purpose |
|---|---|---|
| `{namespace}:stream:{queue}` | Stream | Main message queue |
| `{namespace}:delayed` | Sorted Set | Delayed messages (score = ETA in ms) |
| `{namespace}:dlq:{queue}` | Stream | Dead-letter queue per queue |

Consumer group `workers` on each stream, consumer name `worker-{broker_id}` per process.

## Dashboard

A built-in web dashboard for monitoring queues, inspecting messages, and managing dead-letter queues. Zero additional dependencies.

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

Every 60 seconds, the consumer runs `XAUTOCLAIM` to recover messages stuck in dead consumers' pending entry lists (PEL). This is deterministic, unlike the probabilistic recovery in the built-in broker.

### Message Lifecycle

- **ack**: `XACK` + `XDEL` (removes from PEL and frees stream memory)
- **nack**: `XADD` to DLQ stream, then `XACK` + `XDEL` from main stream
- **requeue**: `XADD` new entry + `XACK` + `XDEL` old entry (atomic via pipeline)

### Delayed Messages (`DelayedScheduler`)

One daemon thread per worker process polls the `{namespace}:delayed` sorted set every second. Due messages (score ≤ now) are atomically removed with `ZREM` and added to their target stream with `XADD`. Multiple processes can safely run schedulers concurrently.

## License

MIT
