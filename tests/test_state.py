import json
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from control_plane.db import DomainError, PoisonMessage, Store
from tests.conftest import event, finish


@pytest.mark.parametrize("operation", ["deploy", "update", "destroy"])
@pytest.mark.parametrize("outcome", ["done", "failed"])
def test_all_terminal_transitions(store, operation, outcome):
    created = store.create("acme", "Acme")
    tenant_id, task = created["tenant"]["id"], created["task"]
    assert created["tenant"]["version"] == 1
    if operation != "deploy":
        finish(store, task["id"])
        updated = store.mutate(tenant_id, operation, version=2, name="New name")
        task = updated["task"]
        assert updated["tenant"]["version"] == 3
        assert (
            updated["tenant"]["status"]
            == {"update": "updating", "destroy": "destroying"}[operation]
        )
    before = store.get("tenant", tenant_id)
    store.apply_progress(event(task["id"], "in_progress"))
    assert store.get("tenant", tenant_id) == before
    assert store.get("task", task["id"])["status"] == "in_progress"
    store.apply_progress(event(task["id"], outcome))
    tenant = store.get("tenant", tenant_id)
    expected = (
        "failed" if outcome == "failed" else ("destroyed" if operation == "destroy" else "active")
    )
    assert tenant["status"] == expected
    assert tenant["version"] == before["version"] + 1
    assert store.get("task", task["id"])["status"] == outcome
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", tenant["updated_at"])


@pytest.mark.parametrize(
    "status", ["provisioning", "updating", "destroying", "destroyed", "failed"]
)
def test_patch_guards_every_nonactive_state(store, status):
    created = store.create("acme", "Acme")
    tenant_id = created["tenant"]["id"]
    with store.transaction() as conn:
        conn.execute("UPDATE tenants SET status = ? WHERE id = ?", (status, tenant_id))
    with pytest.raises(DomainError, match="Cannot update") as exc:
        store.mutate(tenant_id, "update", 1, "new")
    assert exc.value.code == "tenant_update_not_allowed"
    assert len(store.list_resources("task", 100, 0)["items"]) == 1


@pytest.mark.parametrize("status", ["provisioning", "updating", "destroying", "destroyed"])
def test_destroy_guards(store, status):
    tenant_id = store.create("acme", "Acme")["tenant"]["id"]
    with store.transaction() as conn:
        conn.execute("UPDATE tenants SET status = ? WHERE id = ?", (status, tenant_id))
    with pytest.raises(DomainError) as exc:
        store.mutate(tenant_id, "destroy")
    assert exc.value.code == "tenant_update_not_allowed"


def test_failed_can_be_destroyed_and_slug_never_reused(store):
    created = store.create("acme", "Acme")
    finish(store, created["task"]["id"], "failed")
    destroying = store.mutate(created["tenant"]["id"], "destroy")
    finish(store, destroying["task"]["id"])
    with pytest.raises(DomainError) as exc:
        store.create("acme", "Another")
    assert exc.value.code == "tenant_already_exists"


def test_duplicates_and_terminal_are_noops_even_after_new_task(store):
    created = store.create("acme", "Acme")
    task_id, tenant_id = created["task"]["id"], created["tenant"]["id"]
    start, done = event(task_id, "in_progress"), event(task_id, "done")
    assert store.apply_progress(start) == "applied"
    assert store.apply_progress(start) == "duplicate"
    assert store.apply_progress(done) == "applied"
    task = store.get("task", task_id)
    updated = store.mutate(tenant_id, "update", 2, "Changed")
    for stale in (done, start, event(task_id, "failed"), event(task_id, "done")):
        store.apply_progress(stale)
        assert store.get("tenant", tenant_id) == updated["tenant"]
        assert store.get("task", task_id) == task


def test_out_of_order_receipt_survives_restart(store):
    created = store.create("acme", "Acme")
    task_id = created["task"]["id"]
    terminal = event(task_id, "done")
    assert store.apply_progress(terminal) == "pending"
    assert store.apply_progress(terminal) == "duplicate"
    store.apply_progress(event(task_id, "failed"))  # First terminal receipt wins.
    assert store.get("task", task_id)["status"] == "accepted"
    restarted = Store(store.path)
    restarted.initialize()
    assert restarted.get("tenant", created["tenant"]["id"]) == created["tenant"]
    restarted.apply_progress(event(task_id, "in_progress"))
    assert restarted.get("task", task_id)["status"] == "done"
    assert restarted.get("tenant", created["tenant"]["id"])["version"] == 2
    with restarted.connection() as conn:
        assert (
            conn.execute("SELECT count(*) FROM inbox WHERE disposition = 'pending'").fetchone()[0]
            == 0
        )


def test_poison_unknown_task_and_id_collision(store):
    with pytest.raises(PoisonMessage):
        store.apply_progress(event(uuid4(), "done"))
    task = store.create("acme", "Acme")["task"]
    start = event(task["id"], "in_progress")
    store.apply_progress(start)
    with pytest.raises(PoisonMessage):
        store.apply_progress(event(task["id"], "done", start.id))


def race(fn, count=12):
    barrier = Barrier(count)

    def run(i):
        barrier.wait(timeout=10)
        try:
            return fn(i)
        except DomainError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(run, range(count)))


def test_concurrent_slug_creation(store):
    results = race(lambda _: store.create("shared", "Shared"))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert results.count("tenant_already_exists") == 11
    assert len(store.list_resources("task", 100, 0)["items"]) == 1
    with store.connection() as conn:
        assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1


def test_concurrent_patch_same_version(store):
    created = store.create("acme", "Acme")
    finish(store, created["task"]["id"])
    results = race(lambda i: store.mutate(created["tenant"]["id"], "update", 2, f"Name {i}"))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert results.count("tenant_version_conflict") == 11
    assert len(store.list_resources("task", 100, 0)["items"]) == 2


def test_patch_delete_race_one_open_task(store):
    created = store.create("acme", "Acme")
    finish(store, created["task"]["id"])
    results = race(
        lambda i: store.mutate(
            created["tenant"]["id"], "update" if i % 2 else "destroy", 2, "Changed"
        )
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert len(store.list_resources("task", 100, 0)["items"]) == 2


def test_simultaneous_duplicate_results_increment_once(store):
    created = store.create("acme", "Acme")
    start, done = event(created["task"]["id"], "in_progress"), event(created["task"]["id"], "done")
    race(lambda _: store.apply_progress(start))
    race(lambda _: store.apply_progress(done))
    assert store.get("tenant", created["tenant"]["id"])["version"] == 2


def test_atomic_creation_and_post_commit_visibility(store, monkeypatch):
    original = Store._new_task

    def fail_after_task(conn, tenant_id, task_type):
        original(conn, tenant_id, task_type)
        # A separate connection (the publisher) cannot see the uncommitted outbox.
        assert store.pending_event() is None
        raise RuntimeError("Simulated rollback")

    monkeypatch.setattr(Store, "_new_task", staticmethod(fail_after_task))
    with pytest.raises(RuntimeError):
        store.create("acme", "Acme")
    assert store.pending_event() is None
    assert store.list_resources("tenant", 100, 0)["items"] == []
    assert store.list_resources("task", 100, 0)["items"] == []
    monkeypatch.setattr(Store, "_new_task", staticmethod(original))
    created = store.create("acme", "Acme")
    assert json.loads(store.pending_event()["payload"])["id"] == created["task"]["id"]


def test_terminal_receipt_and_tenant_update_rollback_together(store):
    created = store.create("acme", "Acme")
    task_id = created["task"]["id"]
    store.apply_progress(event(task_id, "in_progress"))
    with store.transaction() as conn:
        conn.execute(
            "CREATE TRIGGER fail_update BEFORE UPDATE ON tenants "
            "BEGIN SELECT RAISE(ABORT, 'test rollback'); END"
        )
    done = event(task_id, "done")
    with pytest.raises(sqlite3.IntegrityError):
        store.apply_progress(done)
    assert store.get("task", task_id)["status"] == "in_progress"
    with store.connection() as conn:
        assert (
            conn.execute("SELECT 1 FROM inbox WHERE event_id = ?", (str(done.id),)).fetchone()
            is None
        )
    with store.transaction() as conn:
        conn.execute("DROP TRIGGER fail_update")
    assert store.apply_progress(done) == "applied"


def test_unexpected_constraint_failure_is_not_slug_conflict(store):
    with pytest.raises(sqlite3.IntegrityError):
        store.create("acme", "")
