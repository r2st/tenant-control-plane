import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from control_plane.models import TaskEvent
from control_plane.worker import outcome, parse_args, progress, simulate


def test_worker_ids_and_outcome_are_stable(store):
    task = TaskEvent(**store.create("acme", "Acme")["task"])
    assert progress(task, "done") == progress(task, "done")
    assert progress(task, "done").id != progress(task, "in_progress").id
    assert outcome(task, 0, "seed") == "done"
    assert outcome(task, 1, "seed") == "failed"
    assert outcome(task, 0.5, "seed") == outcome(task, 0.5, "seed")


@pytest.mark.parametrize(
    "argv",
    [
        ["--fail-rate", "-1"],
        ["--fail-rate", "2"],
        ["--fail-rate", "nan"],
        ["--min-delay-ms", "-1"],
        ["--min-delay-ms", "5", "--max-delay-ms", "4"],
    ],
)
def test_invalid_worker_options(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)


@pytest.mark.parametrize("out_of_order", [False, True])
@pytest.mark.parametrize("fail_rate", ["0", "1"])
def test_simulated_worker_publications(store, out_of_order, fail_rate):
    task = TaskEvent(**store.create("acme", "Acme")["task"])
    argv = [
        "--min-delay-ms",
        "1",
        "--max-delay-ms",
        "1",
        "--duplicate-updates",
        "--fail-rate",
        fail_rate,
    ]
    if out_of_order:
        argv.append("--out-of-order")
    channel, connection = Mock(), Mock()
    simulate(
        connection,
        channel,
        SimpleNamespace(delivery_tag=3),
        task.model_dump_json().encode(),
        parse_args(argv),
        threading.Event(),
    )
    messages = [json.loads(call.kwargs["body"]) for call in channel.basic_publish.call_args_list]
    terminal = "done" if fail_rate == "0" else "failed"
    assert [m["status"] for m in messages] == (
        [terminal, terminal, "in_progress", "in_progress"]
        if out_of_order
        else ["in_progress", "in_progress", terminal, terminal]
    )
    assert messages[0]["id"] == messages[1]["id"]
    channel.basic_ack.assert_called_once_with(delivery_tag=3)
    connection.sleep.assert_called_once()


def test_poison_task_and_interrupted_work(store):
    channel = Mock()
    simulate(
        Mock(), channel, SimpleNamespace(delivery_tag=3), b"bad", parse_args([]), threading.Event()
    )
    channel.basic_nack.assert_called_once_with(delivery_tag=3, requeue=False)
    task = TaskEvent(**store.create("acme", "Acme")["task"])
    stop = threading.Event()
    stop.set()
    simulate(
        Mock(),
        channel,
        SimpleNamespace(delivery_tag=3),
        task.model_dump_json().encode(),
        parse_args([]),
        stop,
    )
    channel.basic_ack.assert_not_called()


def test_stop_during_delay_leaves_task_unacked(store):
    task = TaskEvent(**store.create("acme", "Acme")["task"])
    stop, connection, channel = threading.Event(), Mock(), Mock()
    connection.sleep.side_effect = lambda _: stop.set()
    simulate(
        connection,
        channel,
        SimpleNamespace(delivery_tag=3),
        task.model_dump_json().encode(),
        parse_args([]),
        stop,
    )
    assert channel.basic_publish.call_count == 1
    channel.basic_ack.assert_not_called()
