"""Standalone launcher: ``python -m dramatiq_redis_streams.dashboard``."""

import argparse
from wsgiref.simple_server import make_server

from dramatiq_redis_streams import StreamsBroker

from .app import DashboardApp


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Dramatiq Redis Streams Dashboard",
    )
    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6379/0",
        help="Redis connection URL (default: redis://localhost:6379/0)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument("--namespace", default="dramatiq", help="Redis key namespace (default: dramatiq)")
    args = parser.parse_args(argv)

    broker = StreamsBroker(url=args.redis_url, namespace=args.namespace, middleware=[])
    app = DashboardApp(broker)

    httpd = make_server(args.host, args.port, app)
    print(f"Dashboard running at http://{args.host}:{args.port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        broker.close()


if __name__ == "__main__":
    main()
