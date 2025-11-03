# Makefile
DC := docker compose

.PHONY: up down rebuild logs psql etl bash-backend bash-db alembic heads

# --- CLEAN / NUKE / ORPHANS -----------------------------------------------
.PHONY: help clean cache-clear docker-clean orphan-purge reset-db fresh nuke

help:
	@echo "Common targets:"
	@echo "  clean            - Stop stack, remove dangling images, wipe __pycache__"
	@echo "  cache-clear      - Aggressively clear Docker build cache (project-wide safe)"
	@echo "  docker-clean     - Down stack + prune images/build cache"
	@echo "  orphan-purge     - Delete orphan rows (prices/signals not in tickers)"
	@echo "  reset-db         - **DESTRUCTIVE** wipe ./db/data (requires CONFIRM=1)"
	@echo "  fresh            - Clean build + up from scratch (no DB wipe)"
	@echo "  nuke             - **DESTRUCTIVE** Full reset: down -v + rm -rf db/data + prunes (requires Nuke=1)"


.PHONY: tickers-refresh
tickers-refresh:
	docker compose run --rm \
	  -e DATABASE_URL=postgresql+psycopg://postgres:postgres@db:5432/finance \
	  etljob python -m app.etl.tickers_sources --write --db

.PHONY: tickers-count
tickers-count:
	docker compose exec db psql -U postgres -d finance -c "SELECT COUNT(*) FROM tickers;"


clean:
	$(DC) down
	# python caches
	find backend -type d -name "__pycache__" -prune -exec rm -rf {} +
	find backend -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	# Next.js caches if present (optional if you mount)
	-[ -d frontend/.next ] && rm -rf frontend/.next || true
	# remove dangling images only (safe)
	docker image prune -f >/dev/null 2>&1 || true

cache-clear:
	# Clear builder cache (safe, doesn’t remove images/containers)
	docker builder prune -f
	# Optionally remove dangling images too
	docker image prune -f

docker-clean: ## stop + prune caches (non-destructive to ./db/data)
	$(DC) down
	docker image prune -f
	docker builder prune -f

orphan-purge: ## remove orphan prices/signals (not in tickers)
	# Uses the fixer with default behavior (delete orphans).
	$(DC) run --rm migrations python -m app.ops.fix_db --auto

reset-db: ## wipe local Postgres data folder (requires CONFIRM=1)
	@if [ "$(CONFIRM)" != "1" ]; then \
		echo "Refusing: set CONFIRM=1 to proceed (this deletes ./db/data)."; exit 1; \
	fi
	$(DC) down
	rm -rf db/data/*
	mkdir -p db/data
	# Optional: permission fix
	chmod -R u+rwX db/data

fresh: ## clean build + up (keeps existing DB data)
	$(MAKE) docker-clean
	$(DC) build --no-cache
	$(DC) up --build

nuke: ## full reset (requires NUKE=1) – wipes db/data and cache, rebuilds fresh
	@if [ "$(NUKE)" != "1" ]; then \
		echo "Refusing: set NUKE=1 to proceed (this nukes ./db/data and prunes caches)."; exit 1; \
	fi
	$(DC) down -v
	rm -rf db/data/*
	mkdir -p db/data
	docker builder prune -f
	docker image prune -f
	$(DC) build --no-cache
	$(DC) up --build

up:
	$(DC) up --build

down:
	$(DC) down

rebuild:
	$(DC) build --no-cache

logs:
	$(DC) logs -f

psql:
	$(DC) exec db bash -lc 'psql -U postgres -d finance'

etl:
	$(DC) run --rm etljob
etl-db:
	docker compose run --rm etljob python -m app.etl.run_etl
# drop --limit to run all; add --tickers-file path to override with a file when needed
etl-file:
	$(DC) run --rm etljob python -m app.etl.run_etl --tickers-file /app/app/etl/tickers.yaml

# Tighter, safer run when Yahoo is spicy
etl-safe:
	YF_MAX_BATCH=10 YF_SLEEP_SEC=2.0 YF_ADAPTIVE_SLOWSEC=10.0 $(DC) run --rm etljob
bash-backend:
	$(DC) exec backend bash

bash-db:
	$(DC) exec db bash

# run alembic inside the same image/config used by migrations
alembic:
	$(DC) run --rm migrations alembic -c app/migrations/alembic.ini $(CMD)

heads:
	$(DC) run --rm migrations alembic -c app/migrations/alembic.ini heads

audit:
	$(DC) run --rm migrations python -m app.ops.audit_db

fix:
	$(DC) run --rm migrations python -m app.ops.fix_db --auto --normalize-weights

heal:
	# canonical pipe: audit -> fix (reads JSON from stdin)
	AUDIT_JSON=1 $(DC) run --rm migrations python -m app.ops.audit_db | \
	$(DC) run --rm migrations python -m app.ops.fix_db --from-json - --normalize-weights

heal-etl:
	# same as heal, but allow fixer to run ETL if stale
	AUDIT_JSON=1 $(DC) run --rm migrations python -m app.ops.audit_db | \
	$(DC) run --rm migrations python -m app.ops.fix_db --from-json - --normalize-weights --run-etl

