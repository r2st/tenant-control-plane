"""Exercise the live HTTP service, including a twelve-client optimistic-lock race."""

import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4


def request(method: str, path: str, body=None):
    base = os.getenv("API_URL", "http://localhost:8000")
    payload = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:  # nosec B310
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def wait_for(path: str, target: str):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status, data = request("GET", path)
        if status == 200 and data["status"] == target:
            return data
        if data.get("status") == "failed":
            raise RuntimeError("Worker failed the task; run smoke with WORKER_FAIL_RATE=0")
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for {path} -> {target}")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    slug = "smoke-" + uuid4().hex[:12]
    status, created = request("POST", "/tenants", {"slug": slug, "name": "Smoke test"})
    require(status == 202, f"Create failed: {created}")
    path = f"/tenants/{created['tenant']['id']}"
    active = wait_for(path, "active")
    barrier = Barrier(12)

    def patch(index):
        barrier.wait(timeout=10)
        return request("PATCH", path, {"version": active["version"], "name": f"Winner {index}"})

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(patch, range(12)))
    require(sum(code == 202 for code, _ in results) == 1, "Expected exactly one PATCH winner")
    require(sum(code == 409 for code, _ in results) == 11, "Expected eleven PATCH conflicts")
    wait_for(path, "active")
    status, _ = request("DELETE", path)
    require(status == 202, "DELETE failed")
    final = wait_for(path, "destroyed")
    require(final["version"] == 6, f"Unexpected final version: {final}")
    status, duplicate = request("POST", "/tenants", {"slug": slug, "name": "Reuse"})
    require(status == 409 and duplicate["error"]["code"] == "tenant_already_exists", "Slug reused")
    _, tasks = request("GET", f"/tasks?tenant_id={final['id']}")
    require(len(tasks["items"]) == 3, "Unexpected task count")
    require(all(task["status"] == "done" for task in tasks["items"]), "Unfinished tasks")
    print(
        json.dumps(
            {
                "result": "PASS",
                "tenant_id": final["id"],
                "slug": slug,
                "lifecycle": "deploy -> update -> destroy",
                "final_version": final["version"],
                "concurrent_patches": {"accepted": 1, "conflicts": 11},
                "completed_tasks": 3,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
