# Facebook Pages DLT Data Extraction Pipeline Makefile

.PHONY: help install setup extract validate clean test export

help:  ## Show this help message
	@echo "Facebook Pages DLT Data Extraction Pipeline Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install Python dependencies
	pip install -r requirements.txt

setup:  ## Run the complete setup and validation
	python setup_pipeline.py

extract:  ## Extract data from Facebook API
	python facebook_pages_pipeline.py

validate:  ## Validate pipeline output
	python validate_pipeline.py

pipeline: extract validate  ## Run the complete extraction pipeline

export:  ## Export extracted data to various formats
	@echo "Available export formats:"
	@echo "  - DuckDB database: facebook_pages_pipeline.duckdb"
	@echo "  - Parquet files: Available via DLT export commands"
	@echo "  - CSV export: Use DuckDB CLI or Python scripts"

clean:  ## Remove cache files and build artifacts
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf .dlt/pipeline_state
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

test:  ## Run tests
	pytest

dev-setup:  ## Setup development environment
	python -m venv venv
	source venv/bin/activate && pip install -r requirements.txt
	@echo "Development environment ready! Activate with: source venv/bin/activate"
	@echo "Configure your Facebook credentials in .dlt/secrets.toml"

check-credentials:  ## Verify Facebook API credentials
	python -c "import dlt; print('Credentials configured!' if dlt.secrets.get('facebook_refresh_token') and dlt.secrets.get('facebook_page_id') else 'Please configure credentials in .dlt/secrets.toml')"