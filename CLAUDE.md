# Rejected

Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon.

## Development

```bash
UV_CONFIG_FILE=/dev/null uv sync --all-groups   # Install all dependencies
UV_CONFIG_FILE=/dev/null uv run coverage run    # Run tests with coverage
UV_CONFIG_FILE=/dev/null uv run coverage report # View coverage report
UV_CONFIG_FILE=/dev/null uv run pre-commit run -a  # Run linting
```

## Documentation

```bash
UV_CONFIG_FILE=/dev/null uv run mkdocs serve    # Serve docs locally at http://127.0.0.1:8000
UV_CONFIG_FILE=/dev/null uv run mkdocs build    # Build docs to site/
```

## Code Style

- Ruff for linting and formatting (configured in pyproject.toml)
- Single quotes for strings
- 79 character line length
- `rejected/consumer.py` has intentional suppression of B904/C901 for Tornado gen.Return pattern

## Notes

- Use `UV_CONFIG_FILE=/dev/null` for all uv commands — this repo is on github.com, not the internal AWeber PyPI
- Tests use unittest discovery with `-s tests -t .` to support relative imports in the test package
- `pkg_resources` has been replaced with `importlib.metadata` throughout
