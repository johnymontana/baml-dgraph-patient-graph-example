.PHONY: help install setup test extract import clean start-dgraph

help: ## Show this help message
	@echo "Medical Data Extraction with BAML and DGraph"
	@echo "=========================================="
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies using uv
	uv sync

install-dev: ## Install development dependencies
	uv sync --extra dev

setup: ## Run the setup script
	python setup.py

test: ## Run the test script to verify setup
	uv run python test_setup.py

extract: ## Extract medical data using BAML
	uv run python scripts/extract_medical_data.py

import: ## Import extracted data into DGraph
	uv run python scripts/dgraph_importer.py

full-import: ## Import FLIR parquet data into DGraph
	uv run python scripts/import_parquet_data.py

test-full-import: ## Test FLIR parquet import with limited records
	uv run python scripts/test_parquet_import.py

test-duplicate-prevention: ## Test duplicate prevention functionality
	uv run python scripts/test_duplicate_prevention.py

pipeline: extract import ## Run the complete pipeline (extract + import)

start-dgraph: ## Start DGraph using Docker
	docker run -it -p 8080:8080 -p 9080:9080 dgraph/standalone:latest

clean:
	rm -f extracted_*.json data/all_medical_records.json
	rm -rf __pycache__ scripts/__pycache__ .pytest_cache

clean-venv: ## Remove virtual environment
	rm -rf .venv
	rm -f uv.lock

baml-generate: ## Generate BAML Python client
	baml-cli generate

baml-init: ## Initialize BAML project
	baml-cli init

uv-sync: ## Sync dependencies with uv
	uv sync

uv-add: ## Add a new dependency (usage: make uv-add PKG=package_name)
	uv add $(PKG)

uv-add-dev: ## Add a new development dependency (usage: make uv-add-dev PKG=package_name)
	uv add --dev $(PKG)

uv-remove: ## Remove a dependency (usage: make uv-remove PKG=package_name)
	uv remove $(PKG)

uv-demo: ## Demonstrate uv usage
	uv run python scripts/run_with_uv.py

full-setup: install baml-init baml-generate setup ## Complete project setup

dev-setup: install-dev baml-init baml-generate setup ## Setup with development dependencies
