.PHONY: install up down logs ingest replay test lint serve schema-demo \
	web-install web-dev web-test web-build capture

CAPTURE_MAX ?= 6000

install: web-install
	uv sync --extra dev

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f spark producer api

ingest:
	uv run wikipulse ingest

replay:
	uv run wikipulse replay tests/fixtures/recentchange.jsonl --retime

serve:
	uv run uvicorn wikipulse.api:app --reload --port 8000

test:
	uv run --extra dev pytest -q

lint:
	uv run --extra dev ruff check .

schema-demo:
	docker compose stop spark
	docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.2 /app/scripts/schema_demo.py
	docker compose up -d spark

web-install:
	npm --prefix web ci

web-dev:
	npm --prefix web run dev

web-test:
	npm --prefix web test

web-build:
	npm --prefix web run build

# Dump wiki.edits.raw from the running stack into the demo's recorded capture
# and regenerate the Python→TypeScript parity vectors.
capture:
	mkdir -p data
	docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 --topic wiki.edits.raw --from-beginning \
		--max-messages $(CAPTURE_MAX) --timeout-ms 10000 > data/capture-raw.jsonl || true
	PYTHONPATH=src uv run python scripts/export_capture.py data/capture-raw.jsonl
