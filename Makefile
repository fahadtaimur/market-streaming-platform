.PHONY: install lint format test test-int test-all audit nb-lint nb-format nb-clean hooks help

install:
	uv sync --dev

hooks:
	uv run pre-commit install

lint:
	uv run pre-commit run ruff --all-files

format:
	uv run pre-commit run ruff-format --all-files

test:
	uv run pytest tests/ -v

test-int:
	uv run pytest -m integration -v

test-all:
	uv run pytest -v

nb-lint:
	uv run nbqa "ruff check" notebooks/ --fix

nb-format:
	uv run nbqa "ruff format" notebooks/

nb-clean:
	uv run nbqa "ruff check" notebooks/ --fix && uv run nbqa "ruff format" notebooks/

audit:
	uv run pip-audit

help:
	@echo "install          install all dependencies"
	@echo "hooks            install pre-commit hooks"
	@echo "lint             lint src/ via pre-commit"
	@echo "format           format src/ via pre-commit"
	@echo "test             run unit tests"
	@echo "test-int         run integration tests only"
	@echo "test-all         run all tests"
	@echo "nb-lint          lint notebook cells"
	@echo "nb-format        format notebook cells"
	@echo "nb-clean         lint + format notebooks in one step"
	@echo "audit            check dependencies for vulnerabilities"
