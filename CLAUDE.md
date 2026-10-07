# WikiPulse project note

Status: implementation present; run `make test`, `make lint`, and a Docker integration smoke test before claiming a verified live stack. This folder is a sibling project inside Portfolio Project; avoid touching existing `portfolio-codex` changes.

Stack: Wikimedia EventStreams, Python 3.11 app (Python 3.10 inside the Spark image), Kafka 4.0.0, Spark 4.0.4, Iceberg 1.10.2, FastAPI, static dashboard. Java 17 is inside the Spark image, avoiding the host's Java 26.

Web demo: `web/` (Vite + React + TS) mirrors the contract, score and alert rules in `web/src/core/`; `web/tests/parity.test.ts` must stay green against vectors from `scripts/export_capture.py`. Any scoring or rule change goes in `events.py`, `stream.py` and `web/src/core/` together. GitHub Pages deploys `web/dist` via `.github/workflows/pages.yml`.

Commands: `make up`, `make logs`, `make down`, `make test`, `make lint`, `make replay`, `make schema-demo`, `make web-install`, `make web-test`, `make web-dev`, `make capture`. See `docs/runbook.md` for assertions and recovery. Do not commit `.env`, `data/`, `warehouse/`, or `checkpoints/`.
