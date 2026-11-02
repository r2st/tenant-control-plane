"""Separate simulator process; owns no database connection."""

import argparse
import hashlib
import logging
import os
import random
import signal
import threading
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from control_plane.broker import RESULTS, TASKS, publish, retry_loop
from control_plane.models import ProgressEvent, TaskEvent

LOG = logging.getLogger(__name__)


def progress(task: TaskEvent, status: str) -> ProgressEvent:
    # A task redelivery regenerates exactly the same event ids, even after process restart.
    return ProgressEvent(
        id=uuid5(NAMESPACE_URL, f"tenant-control:{task.id}:{status}"),
        task_id=task.id,
        status=status,
    )


def outcome(task: TaskEvent, fail_rate: float, seed: str) -> str:
    sample = int.from_bytes(hashlib.sha256(f"{seed}:{task.id}".encode()).digest(), "big") / 2**256
    return "failed" if sample < fail_rate else "done"


def simulate(connection, channel, method, body: bytes, args, stop: threading.Event) -> None:
    try:
        task = TaskEvent.model_validate_json(body)
    except ValidationError:
        LOG.warning("Rejecting invalid task to dead-letter queue")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    terminal = outcome(task, args.fail_rate, args.seed)
    states = [terminal, "in_progress"] if args.out_of_order else ["in_progress", terminal]
    for status in states:
        if stop.is_set():
            return  # Leave unacked; the broker redelivers on connection close.
        event = progress(task, status)
        for _ in range(2 if args.duplicate_updates else 1):
            publish(channel, RESULTS, event.model_dump_json(), str(event.id))
        LOG.info("Task %s -> %s", task.id, status)
        if status == states[0]:
            remaining = random.uniform(args.min_delay_ms, args.max_delay_ms) / 1000  # nosec B311
            while remaining > 0 and not stop.is_set():
                interval = min(remaining, 0.25)
                connection.sleep(interval)  # Service AMQP heartbeats even during long work.
                remaining -= interval
    channel.basic_ack(delivery_tag=method.delivery_tag)


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-rate", type=float, default=float(os.getenv("WORKER_FAIL_RATE", "0"))
    )
    parser.add_argument(
        "--min-delay-ms",
        type=int,
        default=int(os.getenv("WORKER_MIN_DELAY_MS", "1000")),
    )
    parser.add_argument(
        "--max-delay-ms",
        type=int,
        default=int(os.getenv("WORKER_MAX_DELAY_MS", "2000")),
    )
    parser.add_argument("--seed", default=os.getenv("WORKER_SEED", "simulator"))
    parser.add_argument(
        "--duplicate-updates",
        action="store_true",
        default=os.getenv("WORKER_DUPLICATE_UPDATES", "0") == "1",
    )
    parser.add_argument(
        "--out-of-order",
        action="store_true",
        default=os.getenv("WORKER_OUT_OF_ORDER", "0") == "1",
    )
    args = parser.parse_args(argv)
    if not 0 <= args.fail_rate <= 1:
        parser.error("fail-rate must be between 0 and 1")
    if args.min_delay_ms < 0 or args.max_delay_ms < args.min_delay_ms:
        parser.error("delays must satisfy 0 <= min-delay-ms <= max-delay-ms")
    return args


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    def consume(connection, channel, stop):
        for method, _, body in channel.consume(TASKS, inactivity_timeout=0.25):
            if stop.is_set():
                break
            if method is not None:
                simulate(connection, channel, method, body, args, stop)
            yield

    retry_loop(consume, stop, key="worker")


if __name__ == "__main__":
    main()
