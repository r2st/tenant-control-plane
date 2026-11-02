import sqlite3
from uuid import uuid4

import pytest

from tests.conftest import finish
from tests.test_state import race


def test_root_opens_interactive_api_docs(client):
    redirect = client.get("/", follow_redirects=False)
    assert redirect.status_code == 307
    assert redirect.headers["location"] == "/docs"
    docs = client.get("/")
    assert docs.status_code == 200
    assert docs.url.path == "/docs"
    assert docs.headers["content-type"].startswith("text/html")
    assert "SwaggerUIBundle" in docs.text
    paths = client.get("/openapi.json").json()["paths"]
    assert "/tenants" in paths
    assert "/" not in paths


def test_crud_http_contract(client):
    response = client.post("/tenants", json={"slug": "acme", "name": " Acme "})
    assert response.status_code == 202
    created = response.json()
    tenant_id, task_id = created["tenant"]["id"], created["task"]["id"]
    assert response.headers["Location"] == f"/tenants/{tenant_id}"
    assert client.get(response.headers["Location"]).json() == created["tenant"]
    assert created["tenant"]["name"] == "Acme"
    assert client.get(f"/tasks/{task_id}").json() == created["task"]
    finish(client.app.state.store, task_id)
    response = client.patch(f"/tenants/{tenant_id}", json={"version": 2, "name": "Changed"})
    assert response.status_code == 202
    assert response.json()["tenant"]["name"] == "Changed"
    finish(client.app.state.store, response.json()["task"]["id"])
    response = client.delete(f"/tenants/{tenant_id}")
    assert response.status_code == 202
    finish(client.app.state.store, response.json()["task"]["id"])
    assert client.get(f"/tenants/{tenant_id}").json()["status"] == "destroyed"
    assert client.get(f"/tenants/{tenant_id}").json()["version"] == 6
    assert {
        task["type"] for task in client.get(f"/tasks?tenant_id={tenant_id}").json()["items"]
    } == {
        "deploy",
        "update",
        "destroy",
    }


@pytest.mark.parametrize(
    "slug", ["ab", "a" * 29, "Abc", "-abc", "abc-", "a_b", "1ab", "a b", "abc\n"]
)
def test_slug_validation(client, slug):
    assert client.post("/tenants", json={"slug": slug, "name": "Acme"}).status_code == 422


@pytest.mark.parametrize("slug", ["abc", "a-b", "a" * 28, "a01"])
def test_valid_slug_boundaries(client, slug):
    assert client.post("/tenants", json={"slug": slug, "name": "Acme"}).status_code == 202


@pytest.mark.parametrize(
    "body",
    [
        {"slug": "abc", "name": ""},
        {"slug": "abc", "name": "   "},
        {"slug": "abc"},
        {"slug": "abc", "name": None},
        {"slug": "abc", "name": "Acme", "status": "active"},
    ],
)
def test_invalid_create(client, body):
    response = client.post("/tenants", json=body)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.parametrize(
    "body",
    [
        {"name": "new"},
        {"version": 1},
        {"version": 1, "name": ""},
        {"version": "1", "name": "new"},
        {"version": True, "name": "new"},
        {"version": 0, "name": "new"},
        {"version": 1, "name": "new", "slug": "another"},
    ],
)
def test_invalid_patch(client, body):
    assert client.patch(f"/tenants/{uuid4()}", json=body).status_code == 422


def test_structured_errors(client):
    missing = str(uuid4())
    for method, path, kwargs, code, status in [
        ("get", f"/tenants/{missing}", {}, "tenant_not_found", 404),
        (
            "patch",
            f"/tenants/{missing}",
            {"json": {"version": 1, "name": "New"}},
            "tenant_not_found",
            404,
        ),
        ("delete", f"/tenants/{missing}", {}, "tenant_not_found", 404),
        ("get", f"/tasks/{missing}", {}, "task_not_found", 404),
        ("get", "/unknown", {}, "http_error", 404),
        ("post", "/tasks", {}, "http_error", 405),
        ("patch", f"/tasks/{missing}", {}, "http_error", 405),
        ("delete", f"/tasks/{missing}", {}, "http_error", 405),
    ]:
        response = getattr(client, method)(path, **kwargs)
        assert response.status_code == status
        assert response.json()["error"]["code"] == code
    created = client.post("/tenants", json={"slug": "abc", "name": "Acme"}).json()
    tenant_id = created["tenant"]["id"]
    duplicate = client.post("/tenants", json={"slug": "abc", "name": "Acme"})
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "tenant_already_exists"
    guarded = client.patch(f"/tenants/{tenant_id}", json={"version": 1, "name": "New"})
    assert guarded.status_code == 409
    assert guarded.json()["error"]["code"] == "tenant_update_not_allowed"
    finish(client.app.state.store, created["task"]["id"])
    stale = client.patch(f"/tenants/{tenant_id}", json={"version": 1, "name": "New"})
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "tenant_version_conflict"


def test_pagination_and_filters(client):
    first = client.post("/tenants", json={"slug": "abc", "name": "One"}).json()
    client.post("/tenants", json={"slug": "def", "name": "Two"})
    finish(client.app.state.store, first["task"]["id"])
    page = client.get("/tenants?limit=1").json()
    assert len(page["items"]) == 1
    assert page["next_offset"] == 1
    second = client.get("/tenants?limit=1&offset=1").json()
    assert second["next_offset"] is None
    assert second["items"][0]["id"] != page["items"][0]["id"]
    assert client.get("/tenants?status=active").json()["items"][0]["id"] == first["tenant"]["id"]
    assert len(client.get("/tasks?status=done").json()["items"]) == 1
    assert len(client.get(f"/tasks?tenant_id={uuid4()}").json()["items"]) == 0
    for path in [
        "/tenants?limit=0",
        "/tasks?limit=101",
        "/tasks?offset=-1",
        "/tenants?status=oops",
    ]:
        assert client.get(path).status_code == 422


def test_actual_http_handler_concurrency(client):
    responses = race(lambda _: client.post("/tenants", json={"slug": "race", "name": "Race"}))
    assert [r.status_code for r in responses].count(202) == 1
    assert [r.status_code for r in responses].count(409) == 11
    created = next(r.json() for r in responses if r.status_code == 202)
    finish(client.app.state.store, created["task"]["id"])
    results = race(
        lambda i: client.patch(
            f"/tenants/{created['tenant']['id']}", json={"version": 2, "name": f"Race {i}"}
        )
    )
    assert [r.status_code for r in results].count(202) == 1
    assert [r.status_code for r in results].count(409) == 11


def test_health_and_exception_envelopes(client, monkeypatch):
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 200

    def unavailable(*args):
        raise sqlite3.OperationalError("internal detail")

    monkeypatch.setattr(client.app.state.store, "get", unavailable)
    response = client.get(f"/tenants/{uuid4()}")
    assert response.status_code == 503
    assert "internal detail" not in response.text

    def bug(*args):
        raise RuntimeError("private detail")

    monkeypatch.setattr(client.app.state.store, "get", bug)
    response = client.get(f"/tenants/{uuid4()}")
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert "private detail" not in response.text
