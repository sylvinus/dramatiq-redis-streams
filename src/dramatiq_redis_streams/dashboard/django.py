"""Django integration helper — mounts the dashboard inside Django URL routing."""


def get_urlpatterns(broker, prefix="dramatiq/"):
    """Return Django URL patterns that mount the dashboard.

    Usage::

        # urls.py
        from dramatiq_redis_streams.dashboard import get_urlpatterns
        urlpatterns += get_urlpatterns(broker)

    Parameters:
        broker: A :class:`~dramatiq_redis_streams.StreamsBroker` instance.
        prefix: URL prefix (default ``"dramatiq/"``).
    """
    from django.urls import re_path
    from django.views.decorators.csrf import csrf_exempt

    from .app import DashboardApp

    wsgi_app = DashboardApp(broker, prefix=prefix)

    def _view(request, path=""):
        from django.http import HttpResponse

        # Build a minimal WSGI environ from Django's request
        environ = request.META.copy()
        environ["PATH_INFO"] = request.path

        status_holder = {}
        headers_holder = {}

        def start_response(status, headers):
            status_holder["status"] = status
            headers_holder["headers"] = headers

        body_parts = wsgi_app(environ, start_response)
        body = b"".join(body_parts)

        status_code = int(status_holder["status"].split(" ", 1)[0])
        response = HttpResponse(body, status=status_code)
        for header, value in headers_holder["headers"]:
            response[header] = value
        return response

    csrf_view = csrf_exempt(_view)

    prefix_stripped = prefix.strip("/")
    if prefix_stripped:
        prefix_stripped += "/"

    return [
        re_path(rf"^{prefix_stripped}(?P<path>.*)$", csrf_view),
    ]
