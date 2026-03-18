.PHONY: up down build pipeline test lint clean

# Start the full stack (db + dashboard)
up:
	docker compose up --build

# Start in background
up-detached:
	docker compose up --build -d

# Stop all containers
down:
	docker compose down

# Rebuild containers without cache
build:
	docker compose build --no-cache

# Run the ETL pipeline manually (requires db to be running)
pipeline:
	python etl/pipeline.py

# Run pipeline skipping Amazon BSR fetch
pipeline-no-amazon:
	python etl/pipeline.py --skip-amazon

# Run full test suite
test:
	python -m pytest tests/ -v

# Run tests with coverage report
test-cov:
	python -m pytest tests/ -v --cov=etl --cov=db --cov-report=term-missing

# Wipe the database volume (full reset)
db-reset:
	docker compose down -v
	docker compose up --build -d

# Connect to the database directly
db-shell:
	docker compose exec db psql -U $${DB_USER} -d $${DB_NAME}

# Show all tables
db-tables:
	docker compose exec db psql -U $${DB_USER} -d $${DB_NAME} -c "\dt"

# Show latest niche scores
db-scores:
	docker compose exec db psql -U $${DB_USER} -d $${DB_NAME} -c \
	"SELECT category_name, opportunity_score, recommendation FROM niche_scores ORDER BY opportunity_score DESC;"

# Clean up python cache files
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
