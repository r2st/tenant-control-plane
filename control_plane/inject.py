"""Publish test messages using the same AMQP path as the worker."""

import argparse
from uuid import uuid4

from control_plane.broker import RESULTS, TASKS, connect, publish, topology
from control_plane.models import ProgressEvent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id")
    parser.add_argument("--status", choices=["in_progress", "done", "failed"])
    parser.add_argument("--event-id", default=str(uuid4()))
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--raw", help="Raw body for poison-message testing")
    parser.add_argument("--queue", choices=[TASKS, RESULTS], default=RESULTS)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("repeat must be positive")
    if args.raw is None and (not args.task_id or not args.status):
        parser.error("provide --task-id and --status, or --raw")
    body = (
        args.raw
        if args.raw is not None
        else ProgressEvent(
            id=args.event_id,
            task_id=args.task_id,
            status=args.status,
        ).model_dump_json()
    )
    connection = connect()
    try:
        channel = connection.channel()
        topology(channel)
        for _ in range(args.repeat):
            publish(channel, args.queue, body, args.event_id)
        print(f"Published {args.repeat} message(s), id={args.event_id}, queue={args.queue}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
