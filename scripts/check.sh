#!/bin/sh
set -eu
ruff format --check .
ruff check .
bandit -q -r control_plane
pytest --cov --cov-report=term-missing -q
