"""The quality gate must not turn test or broker-shutdown failures into success."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "checks_exit, broker_exit, expected",
    [(0, 0, 0), (7, 0, 7), (0, 137, 1), (7, 137, 7)],
)
def test_runner_preserves_failures_and_always_cleans_up(
    tmp_path, checks_exit, broker_exit, expected
):
    command_log = tmp_path / "docker-calls.jsonl"
    docker = tmp_path / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['COMMAND_LOG'], 'a') as log:\n"
        "    log.write(json.dumps(args) + '\\n')\n"
        "if args[0] == 'inspect':\n"
        "    print(os.environ['BROKER_EXIT'])\n"
        "elif args[-4:] == ['ps', '-a', '-q', 'broker']:\n"
        "    print('test-broker-id')\n"
        "elif 'run' in args:\n"
        "    sys.exit(int(os.environ['CHECKS_EXIT']))\n"
    )
    docker.chmod(0o755)
    result = subprocess.run(
        ["sh", "scripts/test.sh"],
        cwd=Path(__file__).resolve().parents[1],
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "COMMAND_LOG": str(command_log),
            "CHECKS_EXIT": str(checks_exit),
            "BROKER_EXIT": str(broker_exit),
        },
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == expected, result.stderr
    calls = [json.loads(line) for line in command_log.read_text().splitlines()]
    assert any("stop" in call and "30" in call for call in calls)
    assert "down" in calls[-1]
    assert any("logs" in call for call in calls) == (expected != 0)
    if broker_exit == 0:
        assert "Broker shutdown: exit code 0." in result.stdout
    else:
        assert "Broker shutdown failed: exit code 137." in result.stderr
