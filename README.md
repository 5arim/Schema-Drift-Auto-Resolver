# DataOps AI: Schema Drift Auto-Resolver

An autonomous Data Engineering agent built with **LangGraph**, **PySpark**, and **FastAPI** that detects Delta Lake schema drift, generates PySpark migration code, validates its own output, and posts progress updates to Slack.

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5-e6642d.svg)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-OSS-green.svg)
![LangGraph](https://img.shields.io/badge/LangGraph-Agentic-black.svg)

## Overview

Schema drift is one of the most common reasons ETL pipelines fail: renamed columns, type changes, or extra fields break strict table contracts.

This project automates the recovery workflow:

1. A pipeline intentionally fails on Day 2 due to drift.
2. The failure log is sent to a LangGraph-based AI service.
3. The agent generates a PySpark patch, validates syntax with `ast`, retries on invalid output, and saves the best patch.
4. Slack notifications provide real-time visibility into the agent lifecycle.

## Key Features

- **Delta schema enforcement failure simulation** to reproduce real production drift incidents.
- **Cyclical LangGraph workflow** with retry routing (not just linear prompt execution).
- **Self-correction loop** with Python syntax validation (`ast.parse`) up to 3 attempts.
- **Cloud-first inference with local fallback**:
  - Primary: OpenRouter via `ChatOpenAI`
  - Backup: local Ollama (`llama3.2:1b`)
- **Slack observability hooks** from inside the agent:
  - incident detected
  - self-correction in progress
  - patch saved

## Project Structure

- `data_generator.py` - Generates synthetic Day 1/Day 2 CSV input with intentional drift
- `pipeline.py` - PySpark + Delta writer that succeeds on Day 1 and fails on Day 2
- `resolver_app.py` - FastAPI + LangGraph AI agent (`/webhook`)
- `orchestrator.py` - Runs pipelines and triggers AI service on Day 2 failure
- `patches/auto_patch_v1.py` - Latest generated migration patch

## Architecture

1. **Simulator** generates:
   - `data/day_1_orders.csv` (baseline schema)
   - `data/day_2_orders.csv` (drifted schema)
2. **Pipeline** appends into Delta table:
   - Day 1 append succeeds
   - Day 2 append throws schema mismatch
3. **Orchestrator** captures Day 2 error log and calls `/webhook`
4. **LangGraph agent**:
   - `parse_error_log` -> extract schema context
   - `generate_patch` -> generate migration code
   - `validate_patch` -> syntax check
   - conditional route:
     - valid -> `save_patch`
     - invalid and retries < 3 -> back to `generate_patch`
     - retries >= 3 -> `save_patch` (best attempt)

## Why LangGraph?

A single LLM call is not enough for production-style data operations. You need state, branching, retries, and deterministic control over flow.

LangGraph enables:

- explicit state fields (`retry_count`, `validation_error`)
- conditional routing for validation-driven retries
- reproducible agent behavior under failure conditions

## Prerequisites

- Python 3.10+
- Java 17 (or compatible for local PySpark runtime)
- OpenRouter API key (recommended)
- (Optional) Ollama installed/running for local fallback
- (Optional) Slack Incoming Webhook URL

## Installation

```bash
git clone https://github.com/yourusername/schema-drift-auto-resolver.git
cd schema-drift-auto-resolver
python -m pip install -r requirements.txt
```

> On Windows (if `python` is not on PATH), use `py` instead of `python`.

## Environment Variables

Create a `.env` file in project root:

```env
OPENROUTER_API_KEY2=your_openrouter_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
```

Notes:

- `OPENROUTER_API_KEY2` is the variable used by `resolver_app.py`.
- If OpenRouter fails/unavailable, the agent falls back to local Ollama.
- If `SLACK_WEBHOOK_URL` is missing, Slack alerts are skipped.

## Quick Start

### 1) Generate synthetic data

```bash
python data_generator.py
```

### 2) Start the AI service

```bash
python resolver_app.py
```

Service runs on `http://localhost:8000`.

### 3) Run orchestration

```bash
python orchestrator.py
```

Expected behavior:

- Day 1 pipeline succeeds
- Day 2 fails with schema drift
- `/webhook` triggers agent
- patch is written to `patches/auto_patch_v1.py`

## API

### `POST /webhook`

Request:

```json
{
  "error_log": "full pipeline error text..."
}
```

Response:

```json
{
  "status": "success",
  "generated_code": "...",
  "saved_path": "patches/auto_patch_v1.py",
  "llm_provider": "openrouter"
}
```

## Troubleshooting

- **500: OPENROUTER key not set**
  - Ensure `.env` contains `OPENROUTER_API_KEY2=...`
  - Restart `resolver_app.py`
- **OpenRouter 401 (User not found / unauthorized)**
  - Regenerate key and update `.env`
- **Fallback fails with WinError 10061**
  - Ollama is not running. Start with `ollama serve`.
- **PySpark startup issues on Windows**
  - Verify Java and Hadoop/winutils setup per your environment.

## Roadmap

- Apply generated patch automatically against ETL codebase
- Add patch quality checks beyond syntax (semantic/schema-aware checks)
- Persist incident history and generated fixes
- Integrate with CI/CD and real schedulers (Airflow/Prefect)

---

If this project helps your team, consider adding real incident samples and extending the validation loop with schema-aware unit tests before auto-apply in production.
