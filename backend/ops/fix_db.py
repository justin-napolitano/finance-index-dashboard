# backend/app/ops/fix_db.py
"""
Finance DB Auto-Fixer

Usage patterns:
  # 1) Pipe audit JSON to fixer (recommended)
  AUDIT_JSON=1 docker compose run --rm migrations python -m app.ops.audit_db \
    | docker compose run --rm migrations python -m app.ops.fix_db --from-json -

  # 2) Run without input: do a quick audit internally, then fix
  docker compose run --rm migrations python -m app.ops.fix_db --auto

Options:
  --from-json -     : read audit JSON from stdin
  --auto            : perform a minimal audit inline then fix
  --adopt-orphans   : instead of deleting orphan prices/signals, create tickers for them
  --normalize-weights : normalize index_constituents weights to sum exactly 1.0
  --run-etl         : if data is stale, run ETL (app.etl.run_etl) inside this process
  --dry-run         : print planned actions; don't modify anything
  --verbose         : more logs

What it can fix (safely):
  - Run alembic upgrade head if schema/alembic is missing or outdated
  - Ensure default index definition ('momentum-10') exists with valid JSON rules
  - Remove duplicate rows in prices/signals (keep one per (ticker,date))
  - Handle orphan rows in prices/signals (delete or adopt tickers)
  - Normalize index_constituents weights per (slug, asof) [optional stricter]
  - Refresh data and index history by running ETL [optional]
"""

from __future__ import annotations
import os, sys, json, argparse, datetime as dt, subprocess
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB

# ---------------------- defaults (mirrors audit) ----------------------
REQUIRED_TABLES = {
    "tickers",
    "prices",
    "signals",
    "index_definitions",
    "index_constituents",
    "index_history",
    "alembic_version",
}
REQUIRED_INDEX_SLUGS = {"momentum-10"}

DEFAULT_RULES = {
    "universe": "US",
    "select": "top",
    "by": "m_score",
    "n": 10,
    "caps": {"sector": 0.35, "ticker": 0.20},
    "floors": {"dollar_vol": 0},
    "weight": "equal",
}

# ---------------------- helpers ----------------------
def log(msg: str, verbose=False):
    ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def die(msg: str, code=1):
    log(f"ERROR: {msg}")
    sys.exit(code)

def q(engine: Engine, sql: str, **params):
    with engine.begin() as conn:
        return conn.execute(text(sql), params)

def fetch_all(engine: Engine, sql: str, **params):
    return q(engine, sql, **params).all()

def fetch_one(engine: Engine, sql: str, **params):
    r = q(engine, sql, **params).first()
    return None if r is None else (r[0] if len(r) == 1 else tuple(r))

def alembic_upgrade_head(verbose=False, dry_run=False):
    cmd = [sys.executable, "-m", "alembic", "-c", "app/migrations/alembic.ini", "upgrade", "head"]
    if dry_run:
        log(f"DRY-RUN: would run: {' '.join(cmd)}", verbose)
        return 0
    log("Running alembic upgrade head ...", verbose)
    return subprocess.call(cmd)

def run_etl(verbose=False, dry_run=False):
    env = os.environ.copy()
    env.setdefault("YFINANCE_USE_CURL", "false")
    env.setdefault("SSL_CERT_FILE", "/usr/local/lib/python3.11/site-packages/certifi/cacert.pem")
    env.setdefault("REQUESTS_CA_BUNDLE", "/usr/local/lib/python3.11/site-packages/certifi/cacert.pem")

    cmd = [sys.executable, "-m", "app.etl.run_etl"]
    if dry_run:
        log(f"DRY-RUN: would run: {' '.join(cmd)}", verbose)
        return 0
    log("Running ETL (app.etl.run_etl) ...", verbose)
    return subprocess.call(cmd, env=env)

def ensure_default_index(engine: Engine, verbose=False, dry_run=False):
    # create momentum-10 if missing; rules as JSONB
    exists = fetch_one(engine, "SELECT 1 FROM index_definitions WHERE slug = :slug LIMIT 1", slug="momentum-10")
    if exists:
        log("index_definitions: 'momentum-10' already present", verbose)
        return
    sql = """
    INSERT INTO index_definitions(slug, name, description, rules, rebalance_freq, reconst_freq)
    VALUES (:slug, :name, :desc, :rules, 'monthly', 'monthly')
    ON CONFLICT (slug) DO NOTHING
    """
    if dry_run:
        log("DRY-RUN: would INSERT default index_definitions row for 'momentum-10'", verbose)
        return
    with engine.begin() as conn:
        conn.execute(
            text(sql).bindparams(
                rules=JSONB.astext.cast(JSONB)  # placeholder to keep types happy if used elsewhere
            ),
            dict(
                slug="momentum-10",
                name="Momentum 10",
                desc="Top 10 by simple momentum score",
                rules=json.dumps(DEFAULT_RULES),
            ),
        )
    log("Inserted default index definition 'momentum-10'", verbose)

def validate_and_fix_rules(engine: Engine, verbose=False, dry_run=False):
    rows = fetch_all(engine, "SELECT slug, rules FROM index_definitions")
    for slug, rules in rows:
        bad = False
        if rules is None:
            bad = True
        else:
            try:
                json.loads(rules if isinstance(rules, str) else json.dumps(rules))
            except Exception:
                bad = True
        if bad:
            log(f"index_definitions.rules invalid for '{slug}' -> writing default rules", verbose)
            if dry_run:
                log("DRY-RUN: would UPDATE rules", verbose)
                continue
            q(engine, "UPDATE index_definitions SET rules = :rules WHERE slug = :slug",
              rules=json.dumps(DEFAULT_RULES), slug=slug)

def remove_duplicate_rows(engine: Engine, table: str, verbose=False, dry_run=False):
    """
    Remove duplicates on (ticker,date), keep one arbitrary row (lowest ctid).
    Postgres-specific (uses ctid).
    """
    if table not in ("prices", "signals"):
        return
    sql = f"""
    DELETE FROM {table} t
    USING (
        SELECT ctid
        FROM (
            SELECT ctid,
                   ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY ctid) AS rn
            FROM {table}
        ) x
        WHERE x.rn > 1
    ) d
    WHERE t.ctid = d.ctid
    """
    if dry_run:
        log(f"DRY-RUN: would delete duplicates from {table}", verbose)
        return
    q(engine, sql)
    log(f"Removed duplicates from {table}", verbose)

def handle_orphans(engine: Engine, adopt=False, verbose=False, dry_run=False):
    if adopt:
        # create minimal tickers for any referenced by prices/signals
        sql_missing = """
        WITH p AS (
            SELECT DISTINCT ticker FROM prices
            UNION
            SELECT DISTINCT ticker FROM signals
        )
        SELECT p.ticker
        FROM p
        LEFT JOIN tickers t ON t.ticker = p.ticker
        WHERE t.ticker IS NULL
        """
        tickers = [r[0] for r in fetch_all(engine, sql_missing)]
        if not tickers:
            log("No orphan tickers to adopt.", verbose)
            return
        if dry_run:
            log(f"DRY-RUN: would INSERT {len(tickers)} missing tickers", verbose)
            return
        # minimal insert
        for tkr in tickers:
            q(engine, "INSERT INTO tickers(ticker) VALUES (:t) ON CONFLICT (ticker) DO NOTHING", t=tkr)
        log(f"Adopted {len(tickers)} orphan tickers into tickers table.", verbose)
    else:
        # delete orphan rows from prices/signals
        for table in ("prices", "signals"):
            sql_del = f"""
            DELETE FROM {table} x
            USING (
                SELECT {table}.ticker
                FROM {table}
                LEFT JOIN tickers t ON t.ticker = {table}.ticker
                WHERE t.ticker IS NULL
            ) d
            WHERE x.ticker = d.ticker
            """
            if dry_run:
                log(f"DRY-RUN: would delete orphan rows from {table}", verbose)
                continue
            res = q(engine, sql_del)
            log(f"Deleted orphan rows from {table} (affected may be 0).", verbose)

def normalize_constituent_weights(engine: Engine, strict=False, verbose=False, dry_run=False):
    """
    Normalize weights per (slug, asof) to sum exactly 1.0.
    """
    sql_keys = "SELECT DISTINCT slug, asof::date FROM index_constituents ORDER BY asof DESC"
    keys = fetch_all(engine, sql_keys)
    for slug, asof in keys:
        rows = fetch_all(engine, """
            SELECT ticker, weight FROM index_constituents
            WHERE slug = :slug AND asof::date = :asof
        """, slug=slug, asof=asof)
        if not rows:
            continue
        total = sum(float(r[1] or 0.0) for r in rows)
        if total == 0.0:
            log(f"Skip normalization for {slug} {asof}: total=0.", verbose)
            continue
        # compute new weights
        updates = [(r[0], float(r[1] or 0.0)/total) for r in rows]
        if dry_run:
            log(f"DRY-RUN: would normalize {slug} {asof} to sum 1.0", verbose)
            continue
        with engine.begin() as conn:
            for tkr, w in updates:
                conn.execute(text("""
                    UPDATE index_constituents
                    SET weight = :w
                    WHERE slug = :slug AND asof::date = :asof AND ticker = :t
                """), dict(w=w, slug=slug, asof=asof, t=tkr))
        log(f"Normalized weights for {slug} {asof} (sum=1.0).", verbose)

# ---------------------- planner ----------------------
def plan_from_audit(audit: Dict[str, Any]) -> Dict[str, Any]:
    # Map audit results to actions.
    rmap = {r["name"]: r for r in audit.get("results", [])}
    actions = {
        "alembic_upgrade": False,
        "ensure_index_def": False,
        "fix_rules": False,
        "dedup_prices": False,
        "dedup_signals": False,
        "fix_orphans": "delete",  # 'delete' | 'adopt' | 'none'
        "normalize_weights": False,
        "run_etl": False,
    }

    # connectivity
    if not rmap.get("connectivity", {}).get("ok", False):
        # can't do anything without connectivity; caller should see failure
        return actions

    # schema / alembic
    if not rmap.get("schema_tables", {}).get("ok", False) or not rmap.get("alembic_head", {}).get("ok", False):
        actions["alembic_upgrade"] = True

    # index definitions
    if not rmap.get("index_definitions", {}).get("ok", False):
        actions["ensure_index_def"] = True
        actions["fix_rules"] = True

    # duplicates
    if not rmap.get("duplicates_prices", {}).get("ok", True):
        actions["dedup_prices"] = True
    if not rmap.get("duplicates_signals", {}).get("ok", True):
        actions["dedup_signals"] = True

    # orphans
    if not rmap.get("orphans_prices", {}).get("ok", True) or not rmap.get("orphans_signals", {}).get("ok", True):
        actions["fix_orphans"] = "delete"  # default behavior

    # weights
    if not rmap.get("constituent_weight_sums", {}).get("ok", True):
        actions["normalize_weights"] = True

    # stale data
    if not rmap.get("prices_recency", {}).get("ok", True) or not rmap.get("signals_recency", {}).get("ok", True) or not rmap.get("index_history_freshness", {}).get("ok", True):
        actions["run_etl"] = True

    return actions

def minimal_audit(engine: Engine) -> Dict[str, Any]:
    """
    Very small audit to support --auto without running full audit module.
    """
    results = []

    # connectivity
    ok = True
    try:
        fetch_one(engine, "SELECT 1")
    except Exception as e:
        ok = False
    results.append({"name": "connectivity", "ok": ok})

    # schema
    tables = {r[0] for r in fetch_all(engine, """
        SELECT table_name FROM information_schema.tables WHERE table_schema='public';
    """)}
    missing = list(REQUIRED_TABLES - tables)
    results.append({"name": "schema_tables", "ok": len(missing) == 0})

    # alembic
    alembic_ok = True
    try:
        _ = fetch_one(engine, "SELECT version_num FROM alembic_version")
    except Exception:
        alembic_ok = False
    results.append({"name": "alembic_head", "ok": alembic_ok})

    # duplicates quick check (counts > 1 per key exist?)
    dup_prices = fetch_one(engine, """
        SELECT COUNT(*) FROM (
          SELECT 1 FROM prices GROUP BY ticker,date HAVING COUNT(*)>1
        ) x
    """)
    dup_signals = fetch_one(engine, """
        SELECT COUNT(*) FROM (
          SELECT 1 FROM signals GROUP BY ticker,date HAVING COUNT(*)>1
        ) x
    """)
    results.append({"name": "duplicates_prices", "ok": (dup_prices == 0)})
    results.append({"name": "duplicates_signals", "ok": (dup_signals == 0)})

    # orphans quick check
    orp_p = fetch_one(engine, """
        SELECT COUNT(*) FROM (
          SELECT p.ticker FROM prices p LEFT JOIN tickers t ON t.ticker=p.ticker WHERE t.ticker IS NULL LIMIT 1
        ) x
    """)
    orp_s = fetch_one(engine, """
        SELECT COUNT(*) FROM (
          SELECT s.ticker FROM signals s LEFT JOIN tickers t ON t.ticker=s.ticker WHERE t.ticker IS NULL LIMIT 1
        ) x
    """)
    results.append({"name": "orphans_prices", "ok": (orp_p == 0)})
    results.append({"name": "orphans_signals", "ok": (orp_s == 0)})

    # weights quick
    bad_weights = fetch_one(engine, """
        SELECT COUNT(*) FROM (
          SELECT slug, asof::date, SUM(weight) s
          FROM index_constituents GROUP BY 1,2
          HAVING SUM(weight) < 0.98 OR SUM(weight) > 1.02
        ) x
    """)
    results.append({"name": "constituent_weight_sums", "ok": (bad_weights == 0)})

    # recency quick: only check presence (actual staleness will trigger ETL optionally)
    has_prices = bool(fetch_one(engine, "SELECT MAX(date)::date FROM prices") is not None)
    has_signals = bool(fetch_one(engine, "SELECT MAX(date)::date FROM signals") is not None)
    has_hist = bool(fetch_one(engine, "SELECT MAX(date)::date FROM index_history") is not None)
    results.append({"name": "prices_recency", "ok": has_prices})
    results.append({"name": "signals_recency", "ok": has_signals})
    results.append({"name": "index_history_freshness", "ok": has_hist})

    return {"results": results}

# ---------------------- main ----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-json", help="Read audit JSON from file path or '-' for stdin")
    ap.add_argument("--auto", action="store_true", help="Run a minimal audit internally before fixing")
    ap.add_argument("--adopt-orphans", action="store_true", help="Insert missing tickers instead of deleting orphan rows")
    ap.add_argument("--normalize-weights", action="store_true", help="Force normalization to sum exactly 1.0")
    ap.add_argument("--run-etl", action="store_true", help="Run ETL if data is stale or history is missing")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        die("DATABASE_URL not set in environment.")

    engine = create_engine(db_url, pool_pre_ping=True, future=True)

    # Load or compute audit
    audit = None
    if args.from_json:
        if args.from_json == "-":
            try:
                audit = json.load(sys.stdin)
            except Exception as e:
                die(f"Failed to read JSON from stdin: {e}")
        else:
            try:
                with open(args.from_json, "r") as f:
                    audit = json.load(f)
            except Exception as e:
                die(f"Failed to read JSON file: {e}")
    elif args.auto:
        audit = minimal_audit(engine)
    else:
        die("Provide --from-json - (stdin) or --auto")

    actions = plan_from_audit(audit)

    # allow flags to refine behavior
    if args.normalize_weights:
        actions["normalize_weights"] = True
    if not args.run_etl:
        # only run ETL if user opted-in
        actions["run_etl"] = False

    adopt = args.adopt_orphans

    # Execute actions
    if not audit.get("results"):
        die("No audit results provided or computed.")

    if not any(actions.values()) and actions.get("fix_orphans") == "delete":
        log("Nothing to fix.", args.verbose)
        return

    # 1) alembic
    if actions["alembic_upgrade"]:
        rc = alembic_upgrade_head(verbose=args.verbose, dry_run=args.dry_run)
        if rc != 0 and not args.dry_run:
            die("Alembic upgrade failed.")

    # 2) ensure index_def + valid rules
    if actions["ensure_index_def"]:
        ensure_default_index(engine, verbose=args.verbose, dry_run=args.dry_run)
    if actions["fix_rules"]:
        validate_and_fix_rules(engine, verbose=args.verbose, dry_run=args.dry_run)

    # 3) duplicates
    if actions["dedup_prices"]:
        remove_duplicate_rows(engine, "prices", verbose=args.verbose, dry_run=args.dry_run)
    if actions["dedup_signals"]:
        remove_duplicate_rows(engine, "signals", verbose=args.verbose, dry_run=args.dry_run)

    # 4) orphans
    if actions["fix_orphans"] in ("delete", "adopt"):
        handle_orphans(engine, adopt=(actions["fix_orphans"] == "adopt" or adopt),
                       verbose=args.verbose, dry_run=args.dry_run)

    # 5) weights normalization
    if actions["normalize_weights"]:
        normalize_constituent_weights(engine, verbose=args.verbose, dry_run=args.dry_run)

    # 6) ETL refresh (opt-in)
    if actions["run_etl"]:
        rc = run_etl(verbose=args.verbose, dry_run=args.dry_run)
        if rc != 0 and not args.dry_run:
            die("ETL run failed.")

    log("Fix routine complete.", args.verbose)

if __name__ == "__main__":
    main()

