from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.db import Store
from control_plane.models import ProgressEvent


@pytest.fixture
def store(tmp_path):
    store = Store(str(tmp_path / "test.sqlite3"))
    store.initialize()
    return store


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "api.sqlite3"), messaging=False)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


def event(task_id, status, event_id=None):
    return ProgressEvent(id=event_id or uuid4(), task_id=task_id, status=status)


def finish(store, task_id, status="done"):
    store.apply_progress(event(task_id, "in_progress"))
    store.apply_progress(event(task_id, status))
