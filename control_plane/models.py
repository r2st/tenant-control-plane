"""Validation shared by the HTTP API and broker consumers."""

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

TenantStatus = Literal["provisioning", "active", "updating", "destroying", "destroyed", "failed"]
TaskStatus = Literal["accepted", "in_progress", "done", "failed"]
TaskType = Literal["deploy", "update", "destroy"]
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Slug = Annotated[str, StringConstraints(pattern=r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$")]


def timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantCreate(StrictModel):
    slug: Slug
    name: Name


class TenantPatch(StrictModel):
    version: int = Field(ge=1, strict=True)
    name: Name


class TaskEvent(StrictModel):
    schema_version: Literal[1] = 1
    id: UUID
    type: TaskType
    tenant_id: UUID
    status: Literal["accepted"]
    created_at: str
    updated_at: str


class ProgressEvent(StrictModel):
    schema_version: Literal[1] = 1
    id: UUID
    task_id: UUID
    status: Literal["in_progress", "done", "failed"]
