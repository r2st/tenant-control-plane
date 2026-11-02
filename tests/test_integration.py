"""These tests use real durable queues and a separate worker OS process."""

import os
import subprocess
import sys
import time
from contextlib import contextmanager
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.broker import RESULTS, TASKS, connect, dispatch_one, publish, topology
from tests.conftest import event

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("INTEGRATION_TESTS") != "1", reason="use make test for RabbitMQ"),
]


def eventually(fn, timeout=15):
    deadline = time.monotonic() + timeout
    value = None
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.03)
    raise AssertionError(f"Condition not reached within {timeout}s; last result={value!r}")


@pytest.fixture
def broker_channel():
    connection = connect()
    channel = connection.channel()
    topology(channel)
    for queue in (TASKS, RESULTS, TASKS + ".dead", RESULTS + ".dead"):
        channel.queue_purge(queue)
    yield channel
    connection.close()


@contextmanager
def worker(tmp_path, *args):
    with (tmp_path / "worker.log").open("w+") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "control_plane.worker", *args],
            stdout=log,
            stderr=log,
        )
        try:
            yield process
            assert process.poll() is None, "worker exited unexpectedly"
        except BaseException:
            log.seek(0)
            print(log.read())
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def wait_status(client, path, status):
    def poll():
        value = client.get(path).json()
        return value if value.get("status") == status else None

    return eventually(poll)


def test_real_broker_full_lifecycle_and_restart(broker_channel, tmp_path):
    db_path = str(tmp_path / "live.sqlite3")
    with (
        TestClient(create_app(db_path)) as client,
        worker(tmp_path, "--min-delay-ms", "300", "--max-delay-ms", "300"),
    ):
        eventually(lambda: client.get("/health/ready").status_code == 200)
        created = client.post("/tenants", json={"slug": "real", "name": "Real"}).json()
        tenant_path = f"/tenants/{created['tenant']['id']}"
        task_path = f"/tasks/{created['task']['id']}"
        wait_status(client, task_path, "in_progress")
        assert wait_status(client, tenant_path, "active")["version"] == 2
        assert client.get(task_path).json()["status"] == "done"
        update = client.patch(tenant_path, json={"version": 2, "name": "Changed"}).json()
        assert update["tenant"]["status"] == "updating"
        assert wait_status(client, tenant_path, "active")["version"] == 4
        delete = client.delete(tenant_path).json()
        assert delete["tenant"]["status"] == "destroying"
        assert wait_status(client, tenant_path, "destroyed")["version"] == 6
        final = client.get(tenant_path).json()
    # New API instance and connections reopen the same disk database.
    with TestClient(create_app(db_path)) as restarted:
        assert restarted.get(tenant_path).json() == final
        assert len(restarted.get("/tasks").json()["items"]) == 3


@pytest.mark.parametrize("fail_rate, expected", [("0", "active"), ("1", "failed")])
def test_real_duplicate_and_reordered_results(broker_channel, tmp_path, fail_rate, expected):
    with (
        TestClient(create_app(str(tmp_path / "order.sqlite3"))) as client,
        worker(
            tmp_path,
            "--fail-rate",
            fail_rate,
            "--duplicate-updates",
            "--out-of-order",
            "--min-delay-ms",
            "250",
            "--max-delay-ms",
            "250",
        ),
    ):
        created = client.post("/tenants", json={"slug": "reordered", "name": "Order"}).json()
        path = f"/tenants/{created['tenant']['id']}"
        tenant = wait_status(client, path, expected)
        assert tenant["version"] == 2
        # Fresh stale id, plus repeat deliveries with exactly the same id.
        stale = event(created["task"]["id"], "in_progress")
        for _ in range(2):
            publish(broker_channel, RESULTS, stale.model_dump_json(), str(stale.id))

        def received():
            with client.app.state.store.connection() as conn:
                return conn.execute(
                    "SELECT 1 FROM inbox WHERE event_id = ?", (str(stale.id),)
                ).fetchone()

        eventually(received)
        assert client.get(path).json() == tenant
        if expected == "failed":
            assert client.delete(path).status_code == 202
            assert wait_status(client, path, "failed")["version"] == 4


def test_poison_messages_do_not_kill_either_consumer(broker_channel, tmp_path):
    with (
        TestClient(create_app(str(tmp_path / "poison.sqlite3"))) as client,
        worker(tmp_path, "--min-delay-ms", "0", "--max-delay-ms", "0"),
    ):
        publish(broker_channel, TASKS, "not json", str(uuid4()))
        publish(broker_channel, RESULTS, "{}", str(uuid4()))
        unknown = event(uuid4(), "done")
        publish(broker_channel, RESULTS, unknown.model_dump_json(), str(unknown.id))
        eventually(
            lambda: (
                broker_channel.queue_declare(
                    queue=TASKS + ".dead", passive=True
                ).method.message_count
                == 1
            )
        )
        eventually(
            lambda: (
                broker_channel.queue_declare(
                    queue=RESULTS + ".dead", passive=True
                ).method.message_count
                == 2
            )
        )
        created = client.post("/tenants", json={"slug": "survivor", "name": "Alive"}).json()
        wait_status(client, f"/tenants/{created['tenant']['id']}", "active")


def test_unacked_task_is_redelivered_and_outbox_recovers(store, broker_channel, tmp_path):
    created = store.create("recovery", "Recovery")
    # No API runtime existed at commit time. A later publisher finds the durable event.
    assert dispatch_one(store, broker_channel)
    assert not dispatch_one(store, broker_channel)
    abandoned = connect()
    channel = abandoned.channel()
    method, _, _ = channel.basic_get(TASKS, auto_ack=False)
    assert method is not None
    abandoned.close()  # Simulate death before acknowledging the task.
    with (
        TestClient(create_app(store.path)) as client,
        worker(tmp_path, "--min-delay-ms", "0", "--max-delay-ms", "0"),
    ):
        tenant = wait_status(client, f"/tenants/{created['tenant']['id']}", "active")
        assert tenant["version"] == 2


def test_readiness_recovers_after_broker_connection_failures(tmp_path, monkeypatch, broker_channel):
    from control_plane import broker

    real_connect = broker.connect
    attempts = 0

    def flaky_connect():
        nonlocal attempts
        attempts += 1
        if attempts <= 4:
            raise OSError("transient network failure")
        return real_connect()

    monkeypatch.setattr(broker, "connect", flaky_connect)
    with (
        TestClient(create_app(str(tmp_path / "retry.sqlite3"))) as client,
        worker(tmp_path, "--min-delay-ms", "0", "--max-delay-ms", "0"),
    ):
        assert client.get("/health/ready").status_code == 503
        created = client.post("/tenants", json={"slug": "offline", "name": "Offline"}).json()
        assert created["tenant"]["status"] == "provisioning"
        eventually(lambda: client.get("/health/ready").status_code == 200)
        wait_status(client, f"/tenants/{created['tenant']['id']}", "active")
        assert attempts >= 6
