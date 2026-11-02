"""Failure reporting and connection cleanup for the operator-facing CLI tools."""

import sys
from unittest.mock import Mock

import pika
import pytest

from control_plane import inject, smoke


@pytest.mark.parametrize("argv", [[], ["--raw", "{}", "--repeat", "0"]])
def test_inject_rejects_incomplete_or_invalid_requests(monkeypatch, capsys, argv):
    monkeypatch.setattr(sys, "argv", ["inject", *argv])
    connect = Mock()
    monkeypatch.setattr(inject, "connect", connect)
    with pytest.raises(SystemExit) as exc:
        inject.main()
    assert exc.value.code == 2
    assert "error:" in capsys.readouterr().err
    connect.assert_not_called()


def test_inject_closes_connection_when_publish_fails(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["inject", "--raw", "{}"])
    connection = Mock()
    monkeypatch.setattr(inject, "connect", lambda: connection)
    monkeypatch.setattr(inject, "publish", Mock(side_effect=pika.exceptions.AMQPConnectionError()))
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        inject.main()
    connection.close.assert_called_once()
    assert "Published" not in capsys.readouterr().out


def test_smoke_reports_failed_provisioning(monkeypatch):
    monkeypatch.setattr(smoke, "request", lambda *_: (200, {"status": "failed"}))
    with pytest.raises(RuntimeError, match="Worker failed the task"):
        smoke.wait_for("/tenants/failed", "active")


def test_smoke_reports_timeout_for_stalled_task(monkeypatch):
    monkeypatch.setattr(smoke.time, "monotonic", Mock(side_effect=[0, 1, 31]))
    monkeypatch.setattr(smoke.time, "sleep", Mock())
    monkeypatch.setattr(smoke, "request", lambda *_: (200, {"status": "provisioning"}))
    with pytest.raises(TimeoutError, match="Timed out waiting"):
        smoke.wait_for("/tenants/stalled", "active")


def test_smoke_fails_instead_of_printing_success_when_create_fails(monkeypatch, capsys):
    request = Mock(return_value=(503, {"error": {"code": "service_unavailable"}}))
    monkeypatch.setattr(smoke, "request", request)
    with pytest.raises(RuntimeError, match="Create failed"):
        smoke.main()
    request.assert_called_once()
    assert "PASS" not in capsys.readouterr().out
