.PHONY: test test-verbose shell build lint clean

test:
	docker compose run --rm --build test

test-verbose:
	docker compose run --rm --build test pytest -v -s --tb=long

shell:
	docker compose run --rm test bash

lint:
	docker compose run --rm test ruff check src/ tests/

lint-fix:
	docker compose run --rm test ruff check --fix src/ tests/

build:
	docker compose build

clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
