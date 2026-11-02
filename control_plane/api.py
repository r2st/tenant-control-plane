"""REST API; no network publication happens inside an HTTP transaction."""

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from control_plane.broker import Runtime
from control_plane.db import DomainError, Store
from control_plane.models import TaskStatus, TenantCreate, TenantPatch, TenantStatus

LOG = logging.getLogger(__name__)


def error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def create_app(db_path: str | None = None, messaging: bool = True) -> FastAPI:
    store = Store(db_path or os.getenv("DATABASE_PATH", "data/control.sqlite3"))
    runtime = Runtime(store)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.initialize()
        if messaging:
            runtime.start()
        yield
        if messaging:
            runtime.close()

    app = FastAPI(title="Tenant Provisioning Control Plane", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.runtime = runtime

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError):
        return error(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError):
        return error(422, "validation_error", "Request does not match the API schema; see /docs")

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        return error(exc.status_code, "http_error", str(exc.detail))

    @app.exception_handler(sqlite3.OperationalError)
    async def database_error(request: Request, exc: sqlite3.OperationalError):
        LOG.exception("Database unavailable")
        return error(503, "service_unavailable", "Database temporarily unavailable; retry later")

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception):
        LOG.exception("Unexpected request failure")
        return error(500, "internal_error", "An unexpected error occurred")

    @app.get("/health/live", tags=["health"])
    def live():
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    def ready():
        with store.connection() as conn:
            conn.execute("SELECT 1 FROM tenants LIMIT 1")
        if messaging and not all(runtime.connected.values()):
            return error(503, "service_unavailable", "Broker connections are recovering")
        return {"status": "ready"}

    @app.post("/tenants", status_code=202, tags=["tenants"])
    def create_tenant(body: TenantCreate, response: Response):
        result = store.create(body.slug, body.name)
        response.headers["Location"] = f"/tenants/{result['tenant']['id']}"
        return result

    @app.get("/tenants", tags=["tenants"])
    def list_tenants(
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
        status: TenantStatus | None = None,
    ):
        return store.list_resources("tenant", limit, offset, status)

    @app.get("/tenants/{tenant_id}", tags=["tenants"])
    def get_tenant(tenant_id: UUID):
        return store.get("tenant", str(tenant_id))

    @app.patch("/tenants/{tenant_id}", status_code=202, tags=["tenants"])
    def patch_tenant(tenant_id: UUID, body: TenantPatch):
        return store.mutate(str(tenant_id), "update", body.version, body.name)

    @app.delete("/tenants/{tenant_id}", status_code=202, tags=["tenants"])
    def delete_tenant(tenant_id: UUID):
        return store.mutate(str(tenant_id), "destroy")

    @app.get("/tasks", tags=["tasks"])
    def list_tasks(
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
        status: TaskStatus | None = None,
        tenant_id: UUID | None = None,
    ):
        return store.list_resources(
            "task", limit, offset, status, str(tenant_id) if tenant_id else None
        )

    @app.get("/tasks/{task_id}", tags=["tasks"])
    def get_task(task_id: UUID):
        return store.get("task", str(task_id))

    return app
