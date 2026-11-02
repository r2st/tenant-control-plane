.PHONY: up down test format logs smoke

# Homebrew's Docker CLI can miss Docker Desktop's credential helpers on macOS.
ifneq ($(wildcard /Applications/Docker.app/Contents/Resources/bin/docker),)
export PATH := /Applications/Docker.app/Contents/Resources/bin:$(PATH)
endif

up:
	docker compose up --build -d --wait

down:
	docker compose down

test:
	sh scripts/test.sh

format:
	uv run ruff format .
	uv run ruff check --fix .

logs:
	docker compose logs -f api worker

smoke:
	docker compose exec -T api python -m control_plane.smoke
