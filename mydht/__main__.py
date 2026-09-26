"""Command line entry point.

    python -m mydht serve [--host H] [--port P] [--join HOST:PORT[,...]]
                          [--replicas N] [--data-dir DIR]
    python -m mydht cluster [--nodes N] [--port P] [--data-dir DIR]

There is no client: the API is plain HTTP, use curl.
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading

from .node import Node


def _wait_for_signal() -> None:
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    stop.wait()


def serve(args) -> None:
    node = Node(args.host, args.port, replicas=args.replicas, bind=args.bind,
                data_dir=args.data_dir).start(args.join)
    print(f"MyDHT node on http://{node.name}/  (Ctrl-C to leave the ring)")
    _wait_for_signal()
    node.stop(leave=True)


def cluster(args) -> None:
    """Run ``--nodes`` nodes in this process on consecutive ports."""
    first = Node(args.host, args.port, replicas=args.replicas, data_dir=args.data_dir).start()
    nodes = [first]
    for i in range(1, args.nodes):
        nodes.append(Node(args.host, args.port + i, data_dir=args.data_dir).start(join=first.name))
    for n in nodes:
        print(f"MyDHT node on http://{n.name}/")
    print("Ctrl-C to stop the cluster")
    _wait_for_signal()
    for n in nodes:
        n.stop(leave=False)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="mydht", description="A small distributed hash table.")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("serve", help="run a single node")
    p.add_argument("--host", default="localhost",
                   help="name other nodes use to reach this one (default: localhost)")
    p.add_argument("--port", type=int, default=50140)
    p.add_argument("--bind", help="address to listen on (default: --host)")
    p.add_argument("--join", metavar="HOST:PORT[,...]",
                   help="join the ring through the first of these nodes that answers; "
                        "if none does, start alone and keep retrying")
    p.add_argument("--replicas", type=int, default=3,
                   help="copies of each key; only used by the first node (default: 3)")
    p.add_argument("--data-dir", metavar="DIR",
                   help="keep data in an SQLite file in DIR (default: memory only)")
    p.set_defaults(func=serve)

    p = sub.add_parser("cluster", help="run several nodes in one process, for trying it out")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=50140, help="first port (default: 50140)")
    p.add_argument("--nodes", type=int, default=5)
    p.add_argument("--replicas", type=int, default=3)
    p.add_argument("--data-dir", metavar="DIR", help="keep data on disk in DIR")
    p.set_defaults(func=cluster)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args.func(args)


if __name__ == "__main__":
    main()
