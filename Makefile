SEED ?= 42

.PHONY: setup dev materialize publish test lint fmt query new-day data reset

setup:
	uv sync --frozen

dev:
	DAGSTER_HOME="$(CURDIR)/.dagster_home" uv run dagster dev

materialize:
	uv run dagster job execute -m cadence.definitions -j refresh_all

publish:
	PUBLISH_REPORT=1 uv run dagster job execute -m cadence.definitions -j refresh_all

test:
	uv run pytest

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

query:
	uv run python scripts/query.py "$(Q)"

new-day:
	cp data/extra/ticket_scans_2025-07-08.csv data/scans/

data:
	uv run python scripts/generate_data.py --seed $(SEED) --out data

reset:
	rm -f data/warehouse/*.duckdb data/warehouse/*.duckdb.wal data/scans/ticket_scans_2025-07-08.csv
	rm -rf data/warehouse/*.duckdb.tmp .tmp_dagster_home_*
