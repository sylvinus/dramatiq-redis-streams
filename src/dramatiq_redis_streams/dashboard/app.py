"""Minimal WSGI application for the Dramatiq Streams dashboard."""

import json
import logging
import re
from urllib.parse import parse_qs

from . import api
from .page import HTML_PAGE

logger = logging.getLogger(__name__)

# Default and hard ceiling for the per-worker pending-message detail list.
_DEFAULT_PENDING_DETAIL = 20
_MAX_PENDING_DETAIL = 200

# Default and hard ceiling for message-listing endpoints.
_DEFAULT_MESSAGE_COUNT = 50
_MAX_MESSAGE_COUNT = 500

_STATUS_PHRASES = {
    200: "OK",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}


class DashboardApp:
    """WSGI application that serves the dashboard UI and API.

    Parameters:
        broker: A :class:`~dramatiq_redis_streams.StreamsBroker` instance.
        prefix: URL prefix the app is mounted at (e.g. ``"/admin/dramatiq"``).
    """

    def __init__(self, broker, prefix=""):
        self.broker = broker
        # Normalise prefix: no trailing slash, leading slash only if non-empty
        prefix = prefix.strip("/")
        self.prefix = ("/" + prefix) if prefix else ""

    # ------------------------------------------------------------------
    # WSGI entry point
    # ------------------------------------------------------------------

    def __call__(self, environ, start_response):
        method = environ["REQUEST_METHOD"]
        path = environ.get("PATH_INFO", "/")

        # Strip prefix
        if self.prefix and path.startswith(self.prefix):
            path = path[len(self.prefix):]
        if not path.startswith("/"):
            path = "/" + path

        for route_method, regex, handler in _ROUTES:
            if method != route_method:
                continue
            m = regex.match(path)
            if m:
                try:
                    return handler(self, environ, start_response, **m.groupdict())
                except Exception:
                    # Log details server-side; don't leak internals to clients.
                    logger.exception("Dashboard request failed: %s %s", method, path)
                    return self._json_response(
                        start_response, 500,
                        {"error": "Internal server error"},
                    )

        # Check if path matches but method is wrong → 405
        for route_method, regex, _handler in _ROUTES:
            if regex.match(path):
                return self._json_response(
                    start_response, 405,
                    {"error": "Method not allowed"},
                )

        return self._json_response(start_response, 404, {"error": "Not found"})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _json_response(self, start_response, status_code, data):
        body = json.dumps(data).encode("utf-8")
        start_response(
            f"{status_code} {_STATUS_PHRASES.get(status_code, 'Error')}",
            [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
        )
        return [body]

    def _html_response(self, start_response, html):
        body = html.encode("utf-8")
        start_response(
            "200 OK",
            [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))],
        )
        return [body]

    @property
    def _client(self):
        return self.broker.client

    @property
    def _namespace(self):
        return self.broker.namespace

    @property
    def _declared(self):
        return self.broker.get_declared_queues()

    @staticmethod
    def _int_param(environ, key, default, maximum):
        """Parse an integer query param, clamped to [0, maximum].

        Bad input falls back to the default rather than 500-ing, and the hard
        ceiling stops a crafted value from triggering an unbounded Redis read.
        """
        qs = parse_qs(environ.get("QUERY_STRING", ""))
        try:
            value = int(qs.get(key, [default])[0])
        except (TypeError, ValueError):
            value = default
        return max(0, min(value, maximum))

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    def _index(self, environ, start_response):
        return self._html_response(start_response, HTML_PAGE)

    def _overview(self, environ, start_response):
        data = api.get_overview(self._client, self._namespace, self._declared)
        return self._json_response(start_response, 200, data)

    def _queue_messages(self, environ, start_response, name=""):
        count = self._int_param(environ, "count", _DEFAULT_MESSAGE_COUNT, _MAX_MESSAGE_COUNT)
        data = api.get_queue_messages(self._client, self._namespace, name, count=count)
        return self._json_response(start_response, 200, data)

    def _dlq_messages(self, environ, start_response, name=""):
        count = self._int_param(environ, "count", _DEFAULT_MESSAGE_COUNT, _MAX_MESSAGE_COUNT)
        data = api.get_dlq_messages(self._client, self._namespace, name, count=count)
        return self._json_response(start_response, 200, data)

    def _delayed(self, environ, start_response):
        data = api.get_delayed_messages(self._client, self._namespace)
        return self._json_response(start_response, 200, data)

    def _requeue(self, environ, start_response, name="", stream_id=""):
        ok = api.requeue_dlq_message(self._client, self._namespace, name, stream_id)
        return self._json_response(start_response, 200, {"ok": ok})

    def _delete(self, environ, start_response, name="", stream_id=""):
        ok = api.delete_dlq_message(self._client, self._namespace, name, stream_id)
        return self._json_response(start_response, 200, {"ok": ok})

    def _purge(self, environ, start_response, name=""):
        count = api.purge_dlq(self._client, self._namespace, name)
        return self._json_response(start_response, 200, {"purged": count})

    def _requeue_all(self, environ, start_response, name=""):
        count = api.requeue_all_dlq(self._client, self._namespace, name)
        return self._json_response(start_response, 200, {"requeued": count})

    def _flush(self, environ, start_response, name=""):
        api.flush_queue(self._client, self._namespace, name)
        return self._json_response(start_response, 200, {"ok": True})

    def _remove(self, environ, start_response, name=""):
        ok = api.remove_queue(self._client, self._namespace, name)
        return self._json_response(start_response, 200, {"ok": ok})

    def _workers(self, environ, start_response):
        # Hard-clamp: the per-message detail fetch costs one Redis round-trip
        # per entry, so an unbounded value would re-introduce the very slowdown
        # this endpoint was fixed to avoid.
        pending_limit = self._int_param(
            environ, "pending", _DEFAULT_PENDING_DETAIL, _MAX_PENDING_DETAIL,
        )
        data = api.get_workers(
            self._client, self._namespace, self._declared,
            pending_limit=pending_limit,
        )
        return self._json_response(start_response, 200, data)

    def _worker_pending(self, environ, start_response, name=""):
        # Seek-paginated drill-down into one worker's pending tasks. Each page
        # is bounded by `count` (same hard ceiling as other listings), and the
        # `after` cursor resumes without re-scanning what came before.
        count = self._int_param(environ, "count", _DEFAULT_MESSAGE_COUNT, _MAX_MESSAGE_COUNT)
        qs = parse_qs(environ.get("QUERY_STRING", ""))
        after = qs.get("after", [None])[0]
        data = api.get_worker_pending(
            self._client, self._namespace, name, self._declared,
            after=after, count=count,
        )
        return self._json_response(start_response, 200, data)


def _r(method, pattern, handler):
    return (method, re.compile(pattern + "$"), handler)


_ROUTES = [
    _r("GET", "/", DashboardApp._index),
    _r("GET", "/api/overview", DashboardApp._overview),
    _r("GET", r"/api/queues/(?P<name>[^/]+)/messages", DashboardApp._queue_messages),
    _r("GET", r"/api/queues/(?P<name>[^/]+)/dlq", DashboardApp._dlq_messages),
    _r("GET", "/api/delayed", DashboardApp._delayed),
    _r("POST", r"/api/queues/(?P<name>[^/]+)/dlq/(?P<stream_id>[^/]+)/requeue", DashboardApp._requeue),
    _r("POST", r"/api/queues/(?P<name>[^/]+)/dlq/(?P<stream_id>[^/]+)/delete", DashboardApp._delete),
    _r("POST", r"/api/queues/(?P<name>[^/]+)/dlq/purge", DashboardApp._purge),
    _r("POST", r"/api/queues/(?P<name>[^/]+)/dlq/requeue-all", DashboardApp._requeue_all),
    _r("POST", r"/api/queues/(?P<name>[^/]+)/flush", DashboardApp._flush),
    _r("POST", r"/api/queues/(?P<name>[^/]+)/remove", DashboardApp._remove),
    _r("GET", "/api/workers", DashboardApp._workers),
    _r("GET", r"/api/workers/(?P<name>[^/]+)/pending", DashboardApp._worker_pending),
]
