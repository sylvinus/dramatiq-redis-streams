"""Minimal WSGI application for the Dramatiq Streams dashboard."""

import json
import re
from urllib.parse import parse_qs

from . import api
from .page import HTML_PAGE

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
                except Exception as exc:
                    return self._json_response(
                        start_response, 500,
                        {"error": str(exc)},
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

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    def _index(self, environ, start_response):
        return self._html_response(start_response, HTML_PAGE)

    def _overview(self, environ, start_response):
        data = api.get_overview(self._client, self._namespace, self._declared)
        return self._json_response(start_response, 200, data)

    def _queue_messages(self, environ, start_response, name=""):
        qs = parse_qs(environ.get("QUERY_STRING", ""))
        count = int(qs.get("count", [50])[0])
        data = api.get_queue_messages(self._client, self._namespace, name, count=count)
        return self._json_response(start_response, 200, data)

    def _dlq_messages(self, environ, start_response, name=""):
        qs = parse_qs(environ.get("QUERY_STRING", ""))
        count = int(qs.get("count", [50])[0])
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

    def _flush(self, environ, start_response, name=""):
        api.flush_queue(self._client, self._namespace, name)
        return self._json_response(start_response, 200, {"ok": True})

    def _workers(self, environ, start_response):
        data = api.get_workers(self._client, self._namespace, self._declared)
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
    _r("POST", r"/api/queues/(?P<name>[^/]+)/flush", DashboardApp._flush),
    _r("GET", "/api/workers", DashboardApp._workers),
]
