# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon. It manages consumer processes that connect to RabbitMQ, receive messages, and process them with user-defined consumer classes.

## Development

```bash
UV_CONFIG_FILE=/dev/null uv sync --all-groups        # Install all dependencies
UV_CONFIG_FILE=/dev/null uv run coverage run          # Run all tests with coverage
UV_CONFIG_FILE=/dev/null uv run coverage report       # View coverage report
UV_CONFIG_FILE=/dev/null uv run pre-commit run -a     # Run linting (ruff format + ruff check)
```

To run a single test:
```bash
UV_CONFIG_FILE=/dev/null .venv/bin/python -m unittest tests.test_consumer.ConsumerExecuteTests.test_execute_calls_process
```

Documentation:
```bash
UV_CONFIG_FILE=/dev/null uv run mkdocs serve          # Serve docs locally at http://127.0.0.1:8000
UV_CONFIG_FILE=/dev/null uv run mkdocs build          # Build docs to site/
```

## Important Notes

- Use `UV_CONFIG_FILE=/dev/null` for all uv commands — this repo is on github.com, not the internal AWeber PyPI
- Tests use unittest discovery with `-s tests -t .` to support relative imports in the test package
- `rejected/consumer.py` has intentional suppression of B904/BLE001/C901 for error handling complexity
- `rejected/process.py` has intentional C901 suppression

## Code Style

- Ruff for linting and formatting (configured in pyproject.toml)
- Single quotes for strings
- 79 character line length

## Architecture

### Process Model

`Controller` (CLI entry point) → `MasterControlProgram` (MCP) → N × `Process` (multiprocessing.Process)

Each `Process` runs an asyncio event loop with one or more pika `AsyncioConnection`s. The MCP polls child processes via `SIGPROF`/`SIGALRM` signals, collects stats via a multiprocessing Queue, and manages process lifecycle (spawn, kill unresponsive, restart on errors).

### Message Flow

1. `Connection` receives a message from RabbitMQ via pika callbacks
2. `Process.on_message` builds a `ProcessingContext` (Pydantic model) and schedules `invoke_consumer`
3. `invoke_consumer` decodes the body via `Codec`, then calls `consumer.execute(ctx)`
4. `_Consumer.execute` runs pre-validation (message type, retry limits), then delegates to `_run_consumer`
5. `Consumer._run_consumer` acquires a lock and calls `prepare()` → `process()` → `finish()`; `TransactionConsumer._run_consumer` calls them without a lock, passing `ctx` as an argument
6. `Process.on_processed` handles the result: ack, nack, requeue, or republish based on the `Result` enum

### Key Module Responsibilities

- **consumer.py**: `_Consumer` base, `Consumer` (locked, self.body-style), `TransactionConsumer` (concurrent, ctx-style)
- **codecs.py**: `Codec` class handles encode/decode dispatch by content_type/content_encoding, plus async Avro schema loading
- **connection.py**: Wraps pika `AsyncioConnection`, manages channel lifecycle, QoS, consumer tags
- **process.py**: `Process(multiprocessing.Process)` — the per-consumer child process with asyncio event loop
- **mcp.py**: `MasterControlProgram` — parent process that spawns/monitors/polls child processes
- **models.py**: All Pydantic models — `Config`, `ConsumerConfig`, `ConnectionConfig`, `Message`, `ProcessingContext`, `Result`
- **state.py**: `State` base class with state machine (INITIALIZING → CONNECTING → IDLE → ACTIVE → SHUTTING_DOWN → STOPPED)
- **testing.py**: `AsyncTestCase(IsolatedAsyncioTestCase)` for consumer unit tests with mocked RabbitMQ
