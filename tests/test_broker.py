import json
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pika
import pytest

from control_plane import broker
from tests.conftest import event


def test_outbox_confirm_failure_and_crash_window(store, monkeypatch):
    task = store.create("acme", "Acme")["task"]
    channel = Mock()
    channel.basic_publish.side_effect = pika.exceptions.AMQPConnectionError()
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        broker.dispatch_one(store, channel)
    assert store.pending_event()["task_id"] == task["id"]
    channel.basic_publish.side_effect = None
    original = store.mark_published
    monkeypatch.setattr(
        store, "mark_published", Mock(side_effect=sqlite3.OperationalError("crash"))
    )
    with pytest.raises(sqlite3.OperationalError):
        broker.dispatch_one(store, channel)
    monkeypatch.setattr(store, "mark_published", original)
    assert broker.dispatch_one(store, channel)
    assert not broker.dispatch_one(store, channel)
    payloads = [json.loads(call.kwargs["body"]) for call in channel.basic_publish.call_args_list]
    assert len(payloads) == 3
    assert all(
        payload["id"] == task["id"] and payload["status"] == "accepted" for payload in payloads
    )
    props = channel.basic_publish.call_args.kwargs["properties"]
    assert props.delivery_mode == 2
    assert props.message_id == task["id"]


def test_consumer_acks_only_after_commit_and_duplicate(store):
    channel, method = Mock(), SimpleNamespace(delivery_tag=7)
    task = store.create("acme", "Acme")["task"]
    body = event(task["id"], "in_progress").model_dump_json().encode()

    def assert_committed(**kwargs):
        assert store.get("task", task["id"])["status"] == "in_progress"

    channel.basic_ack.side_effect = assert_committed
    broker.consume_result(store, channel, method, body)
    broker.consume_result(store, channel, method, body)
    assert channel.basic_ack.call_count == 2
    channel.basic_nack.assert_not_called()


@pytest.mark.parametrize("body", [b"not-json", b"{}", b'{"id":12}', b'"text"'])
def test_poison_is_dead_lettered(store, body):
    channel = Mock()
    broker.consume_result(store, channel, SimpleNamespace(delivery_tag=7), body)
    channel.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
    channel.basic_ack.assert_not_called()


def test_unknown_task_dead_lettered(store):
    channel = Mock()
    broker.consume_result(
        store,
        channel,
        SimpleNamespace(delivery_tag=1),
        event(uuid4(), "done").model_dump_json().encode(),
    )
    channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_database_failure_leaves_unacked(store, monkeypatch):
    channel = Mock()
    monkeypatch.setattr(
        store, "apply_progress", Mock(side_effect=sqlite3.OperationalError("locked"))
    )
    with pytest.raises(sqlite3.OperationalError):
        broker.consume_result(
            store,
            channel,
            SimpleNamespace(delivery_tag=1),
            event(uuid4(), "done").model_dump_json().encode(),
        )
    channel.basic_ack.assert_not_called()
    channel.basic_nack.assert_not_called()


def test_backoff_reconnect_and_shutdown(monkeypatch):
    connection = Mock()
    monkeypatch.setattr(
        broker,
        "connect",
        Mock(
            side_effect=[
                pika.exceptions.AMQPConnectionError(),
                pika.exceptions.AMQPConnectionError(),
                connection,
            ]
        ),
    )
    monkeypatch.setattr(broker, "topology", Mock())
    stop, waits, state = threading.Event(), [], {}
    monkeypatch.setattr(stop, "wait", lambda delay: waits.append(delay))

    def operation(conn, channel, stop):
        assert state["connected"]
        yield
        stop.set()

    broker.retry_loop(operation, stop, state)
    assert len(waits) == 2
    assert 0.25 <= waits[0] <= 0.3125
    assert 0.5 <= waits[1] <= 0.625
    assert not state["connected"]
    connection.close.assert_called_once()
