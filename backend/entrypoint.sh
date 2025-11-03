#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-api}"

echo "[entrypoint] Using DATABASE_URL=${DATABASE_URL:-<unset>}"

# ---------- Ensure Alembic is initialized ----------
if [ ! -f alembic.ini ]; then
  echo "[entrypoint] Creating alembic.ini"
  cat > alembic.ini <<'EOF'
[alembic]
script_location = app/migrations
prepend_sys_path = .

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s %(message)s
EOF
fi

# dir structure (safe if already exists)
mkdir -p app/migrations/versions

# ensure env.py exists (alembic init is idempotent)
alembic init app/migrations >/dev/null 2>&1 || true

# ---------- Make env.py read DATABASE_URL from env ----------
python - <<'PY'
from pathlib import Path
import re, os
p = Path("app/migrations/env.py")
s = p.read_text()
if "import os" not in s:
    s = "import os\n" + s
s = re.sub(r'config\.get_main_option\("sqlalchemy\.url"\)', 'os.environ.get("DATABASE_URL")', s)
p.write_text(s)
print("[entrypoint] env.py wired to DATABASE_URL")
PY

# ---------- If no migrations exist, create a minimal bootstrap ----------
if [ -z "$(ls -A app/migrations/versions 2>/dev/null || true)" ]; then
  REV_ID="init_$(date +%Y%m%d_%H%M%S)"
  echo "[entrypoint] No migrations found → creating $REV_ID"
  alembic revision -m "init tables: index_definitions" --rev-id "$REV_ID" >/dev/null
  cat > app/migrations/versions/${REV_ID}.py <<'PY'
"""init tables: index_definitions"""
from alembic import op
import sqlalchemy as sa

revision = "REPLACE_ME"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        "index_definitions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("slug", sa.String(length=100), nullable=False, unique=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
    )
    op.create_index(
        "ix_index_definitions_slug", "index_definitions", ["slug"], unique=True
    )

def downgrade():
    op.drop_index("ix_index_definitions_slug", table_name="index_definitions")
    op.drop_table("index_definitions")
PY
  sed -i "s/REPLACE_ME/${REV_ID}/" app/migrations/versions/${REV_ID}.py
fi

# ---------- Run migrations (idempotent) ----------
echo "[entrypoint] Running alembic upgrade head…"
alembic upgrade head

# ---------- Optional seed (first-run friendly) ----------
if [ "${SEED_DEFAULTS:-true}" = "true" ]; then
  echo "[entrypoint] Seeding defaults (safe if already present)…"
  python - <<'PY'
import os, psycopg
url = os.environ["DATABASE_URL"]
with psycopg.connect(url) as conn, conn.cursor() as cur:
    cur.execute("""
        insert into index_definitions (slug,name,description) values
        ('sp500','S&P 500','Top 500 US large-cap equities'),
        ('nasdaq100','NASDAQ-100','Tech-heavy large-cap index')
        on conflict (slug) do nothing;
    """)
PY
fi

# ---------- Mode dispatch ----------
if [ "$MODE" = "api" ]; then
  echo "[entrypoint] Starting API…"
  exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
elif [ "$MODE" = "migrate-only" ]; then
  echo "[entrypoint] Migrations complete."
  exit 0
else
  echo "[entrypoint] Unknown mode: $MODE" >&2
  exit 2
fi
