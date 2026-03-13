# Catholic Knowledge Graph — Makefile
# Run `make help` to see available targets.

PYTHON ?= python
PIPELINE = $(PYTHON) pipeline/scripts/run_pipeline.py

.PHONY: help install pipeline resume from quick status clean test lint dev build

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
