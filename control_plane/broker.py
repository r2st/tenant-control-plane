"""Confirmed publications, manual acknowledgements, and reconnect backoff."""

import logging
import os
import random
import sqlite3
import threading
from collections.abc import Callable
from contextlib import suppress

import pika
from pydantic import ValidationError

from control_plane.db import PoisonMessage, Store
from control_plane.models import ProgressEvent

LOG = logging.getLogger(__name__)
TASKS = "control.tasks"
RESULTS = "control.results"
DEAD_EXCHANGE = "control.dead"


def connect() -> pika.BlockingConnection:
    params = pika.ConnectionParameters(
        host=os.getenv("BROKER_HOST", "localhost"),
        port=int(os.getenv("BROKER_PORT", "5672")),
        credentials=pika.PlainCredentials(
            os.getenv("BROKER_USER", "guest"), os.getenv("BROKER_PASSWORD", "guest")
        ),
        heartbeat=30,
        blocked_connection_timeout=5,
        socket_timeout=3,
        stack_timeout=5,
        connection_attempts=1,
    )
    return pika.BlockingConnection(params)


def topology(channel) -> None:
    channel.exchange_declare(exchange=DEAD_EXCHANGE, exchange_type="direct", durable=True)
    for queue in (TASKS, RESULTS):
        channel.queue_declare(queue=queue + ".dead", durable=True)
        channel.queue_bind(queue=queue + ".dead", exchange=DEAD_EXCHANGE, routing_key=queue)
        channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={"x-dead-letter-exchange": DEAD_EXCHANGE},
        )
    channel.basic_qos(prefetch_count=1)
    channel.confirm_delivery()


def publish(channel, queue: str, body: str, event_id: str) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=body,
        mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=event_id,
        ),
    )


def dispatch_one(store: Store, channel) -> bool:
    event = store.pending_event()
    if event is None:
        return False
    # The outbox is visible only after commit. A crash after confirm can replay the same id.
    publish(channel, TASKS, event["payload"], event["task_id"])
    store.mark_published(event["task_id"])
    return True


def consume_result(store: Store, channel, method, body: bytes) -> None:
    try:
        event = ProgressEvent.model_validate_json(body)
        disposition = store.apply_progress(event)
    except (ValidationError, PoisonMessage):
        LOG.warning("Rejecting invalid result to dead-letter queue")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    LOG.info("Result task=%s event=%s disposition=%s", event.task_id, event.id, disposition)
    # A database failure propagates; closing the connection requeues this unacked delivery.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def retry_loop(
    operation: Callable,
    stop: threading.Event,
    state: dict[str, bool] | None = None,
    key: str = "connected",
) -> None:
    backoff = 0.25
    while not stop.is_set():
        connection = None
        try:
            connection = connect()
            channel = connection.channel()
            topology(channel)
            if state is not None:
                state[key] = True
            # Reset only after useful work, not merely after a successful TCP handshake.
            for _ in operation(connection, channel, stop):
                backoff = 0.25
        except (pika.exceptions.AMQPError, OSError, sqlite3.Error):
            LOG.warning("Broker/database unavailable in %s; reconnecting", key, exc_info=True)
        finally:
            if state is not None:
                state[key] = False
            if connection is not None and connection.is_open:
                with suppress(pika.exceptions.AMQPError, OSError):
                    connection.close()
        if not stop.is_set():
            # Randomness is for retry jitter, never for security-sensitive values.
            stop.wait(backoff + random.uniform(0, backoff / 4))  # nosec B311
            backoff = min(backoff * 2, 10)


class Runtime:
    """One publisher and one result consumer, each owning its AMQP connection."""

    def __init__(self, store: Store):
        self.store = store
        self.stop = threading.Event()
        self.connected = {"publisher": False, "consumer": False}
        self.threads: list[threading.Thread] = []

    def _publisher(self, connection, channel, stop):
        while not stop.is_set():
            dispatch_one(self.store, channel)
            connection.process_data_events(time_limit=0.1)
            yield

    def _consumer(self, connection, channel, stop):
        for method, _, body in channel.consume(RESULTS, inactivity_timeout=0.25):
            if stop.is_set():
                break
            if method is not None:
                consume_result(self.store, channel, method, body)
            yield

    def start(self) -> None:
        for key, operation in (("publisher", self._publisher), ("consumer", self._consumer)):
            thread = threading.Thread(
                name=key,
                target=retry_loop,
                args=(operation, self.stop, self.connected, key),
                daemon=True,
            )
            thread.start()
            self.threads.append(thread)

    def close(self) -> None:
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=10)
