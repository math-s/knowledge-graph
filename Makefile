# Catholic Knowledge Graph — Makefile
# Run `make help` to see available targets.

PYTHON ?= python
PIPELINE = $(PYTHON) pipeline/scripts/run_pipeline.py

.PHONY: help install pipeline resume from quick status clean test lint dev build deploy deploy-force corpus-export corpus-load db-push db-pull

help: ## Show this help
	@echo ""
	@echo "  Catholic Knowledge Graph"
	@echo "  ========================"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Pipeline steps:"
	@echo "    1  Ingest CCC            5  Fetch full Bible     9  Build graph       13  Export graph"
	@echo "    2  Parse footnotes       6  Fetch patristic     10  Theme edges       14  Export sources"
	@echo "    3  Assign themes         7  Fetch docs multilang 11  Source nodes"
	@echo "    4  Fetch legacy sources  8  Fetch CCC multilang 12  Compute layout"
	@echo ""

install: ## Install pipeline + web dependencies
	cd pipeline && pip install -e ".[dev]"
	cd web && npm install

# ── Pipeline targets ────────────────────────────────────────────────────────

pipeline: ## Run the full pipeline from scratch
	$(PIPELINE)

resume: ## Resume pipeline from last checkpoint
	$(PIPELINE) --resume

status: ## Show pipeline step completion status
	$(PIPELINE) --list

clean: ## Delete all pipeline checkpoints
	$(PIPELINE) --clean

quick: ## Run pipeline without network fetches (offline)
	$(PIPELINE) --skip-fetch

# Step-specific targets
from: ## Resume from a step: make from S=5
	$(PIPELINE) --from $(S)

step: ## Run a single step: make step S=4
	$(PIPELINE) --only $(S)

# ── Test & lint ─────────────────────────────────────────────────────────────

test: ## Run all pipeline tests
	$(PYTHON) -m pytest pipeline/tests/ -v

lint: ## Lint pipeline code
	$(PYTHON) -m ruff check pipeline/

# ── Web targets ─────────────────────────────────────────────────────────────

dev: ## Start web dev server
	cd web && npm run dev

build: ## Build web for production
	cd web && npm run build

# ── Corpus JSONL ────────────────────────────────────────────────────────────

corpus-export: ## Export DB → data/corpus/ JSONL (run after any DB change)
	$(PYTHON) pipeline/scripts/export_corpus_jsonl.py

corpus-load: ## Load data/corpus/ JSONL → DB (rebuild content tables)
	$(PYTHON) pipeline/scripts/load_corpus_to_db.py

# ── DB sync (S3) ─────────────────────────────────────────────────────────────

db-push: ## Push local DB to S3
	./scripts/sync_db.sh push

db-pull: ## Pull DB from S3 (first-time setup or after a deploy)
	./scripts/sync_db.sh pull

# ── Deploy ──────────────────────────────────────────────────────────────────

deploy: ## Deploy to fly.io, skipping if S3 DB matches what's live
	./scripts/deploy.sh

deploy-force: ## Deploy to fly.io regardless of version match
	./scripts/deploy.sh --force
