# backend/app/ops/audit_db.py
"""
Finance DB Audit & Health Check

Runs a suite of assertions against the Postgres schema used by the finance-index-dashboard.
- Validates required tables exist
- Checks Alembic migration state
- Row counts & basic data sanity
- Duplicates and orphan checks
- Data recency for prices & signals
- Index definitions & rules JSON
- Index constituents weight sums (~= 1.0 per asof)
- Index history presence and freshness

Exit code:
  0 = all checks passed
  1 = some checks failed
"""

from __future__ import annotations
import os, sys, json, math, datetime as dt
from dataclasses import dataclass, field
from typing import List, Tuple, Any, Dict, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
REQUIRED_TABLES = {
    "tickers",
    "prices",
    "signals",
    "index_definitions",
    "index_constituents",
    "index_history",
    "alembic_version",  # migration head
}

# how "fresh" data should be (tune if you only update weekly, etc.)
PRICES_MAX_LAG_DAYS = int(os.getenv("AUDIT_PRICES_MAX_LAG_DAYS", "5"))
SIGNALS_MAX_LAG_DAYS = int(os.getenv("AUDIT_SIGNALS_MAX_LAG_DAYS", "7"))

# which index def should exist by default (per your ETL)
REQUIRED_INDEX_SLUGS = {"momentum-10"}

WEIGHT_TOLERANCE = float(os.getenv("AUDIT_WEIGHT_TOLERANCE", "0.02"))  # ±2%

# -----------------------------------------------------------------------------
# Utility
# -----------------------------------------------------------------------------
@dataclass
class CheckResult:
    name: str
    ok: bool
    details: str = ""
    data: Any = None


@dataclass
class AuditReport:
    started_at: str
    database_url_redacted: str
    results: List[CheckResult] = field(default_factory=list)

    def add(self, name: str, ok: bool, details: str = "", data: Any = None):
        self.results.append(CheckResult(name=name, ok=ok, details=details, data=data))

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "started_at": self.started_at,
            "database_url": self.database_url_redacted,
            "ok": self.ok,
            "results": [
                {"name": r.name, "ok": r.ok, "details": r.details, "data": r.data}
                for r in self.results
            ],
        }

def redacted_db_url(url: str) -> str:
    # redact credentials but keep driver/host/db visible
    try:
        if "://" not in url:
            return "***"
        scheme, rest = url.split("://", 1)
        # remove creds up to '@'
        if "@" in rest:
            rest = rest.split("@", 1)[1]
        return f"{scheme}://***:***@{rest}"
    except:
        return "***"

def q(engine: Engine, sql: str, **params):
    with engine.begin() as conn:
        return conn.execute(text(sql), params)

def fetch_one(engine: Engine, sql: str, **params):
    r = q(engine, sql, **params).first()
    return None if r is None else (r[0] if len(r) == 1 else tuple(r))

def fetch_all(engine: Engine, sql: str, **params):
    return q(engine, sql, **params).all()

# -----------------------------------------------------------------------------
# SQL (kept literal for your documentation preference)
# -----------------------------------------------------------------------------
SQL_HEALTH = "SELECT 1"

SQL_TABLES = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';
"""

SQL_ALEMBIC_HEAD = "SELECT version_num FROM alembic_version"

SQL_COUNTS = """
SELECT 'tickers' AS table, COUNT(*) FROM tickers
UNION ALL SELECT 'prices', COUNT(*) FROM prices
UNION ALL SELECT 'signals', COUNT(*) FROM signals
UNION ALL SELECT 'index_definitions', COUNT(*) FROM index_definitions
UNION ALL SELECT 'index_constituents', COUNT(*) FROM index_constituents
UNION ALL SELECT 'index_history', COUNT(*) FROM index_history;
"""

SQL_MAX_DATES = """
SELECT 'prices' AS table, MAX(date)::date FROM prices
UNION ALL SELECT 'signals', MAX(date)::date FROM signals
UNION ALL SELECT 'index_history', MAX(date)::date FROM index_history;
"""

SQL_DUP_PRICES = """
SELECT ticker, date, COUNT(*) AS c
FROM prices
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY c DESC, date DESC
LIMIT 50;
"""

SQL_DUP_SIGNALS = """
SELECT ticker, date, COUNT(*) AS c
FROM signals
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY c DESC, date DESC
LIMIT 50;
"""

SQL_ORPHAN_PRICES = """
SELECT p.ticker, COUNT(*) AS c
FROM prices p
LEFT JOIN tickers t ON t.ticker = p.ticker
WHERE t.ticker IS NULL
GROUP BY 1
ORDER BY c DESC
LIMIT 50;
"""

SQL_ORPHAN_SIGNALS = """
SELECT s.ticker, COUNT(*) AS c
FROM signals s
LEFT JOIN tickers t ON t.ticker = s.ticker
WHERE t.ticker IS NULL
GROUP BY 1
ORDER BY c DESC
LIMIT 50;
"""

SQL_INDEX_DEFS = """
SELECT slug, name, rules::text
FROM index_definitions
ORDER BY slug;
"""

SQL_CONS_WEIGHT_SUMS = """
SELECT slug, asof::date, SUM(weight) AS weight_sum, COUNT(*) AS n
FROM index_constituents
GROUP BY 1,2
ORDER BY asof DESC, slug
LIMIT 50;
"""

SQL_INDEX_HISTORY_RECENT = """
SELECT slug, MAX(date)::date AS max_date, COUNT(*) AS rows
FROM index_history
GROUP BY slug
ORDER BY max_date DESC;
"""

SQL_SAMPLE_PRICES = """
SELECT ticker, date, close
FROM prices
ORDER BY date DESC
LIMIT 5;
"""

# -----------------------------------------------------------------------------
# Audit steps
# -----------------------------------------------------------------------------
def do_audit(engine: Engine) -> AuditReport:
    report = AuditReport(
        started_at=dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        database_url_redacted=redacted_db_url(os.environ.get("DATABASE_URL", "")),
    )

    # 1) Basic connectivity
    try:
        _ = fetch_one(engine, SQL_HEALTH)
        report.add("connectivity", True, "SELECT 1 ok")
    except Exception as e:
        report.add("connectivity", False, f"failed: {e}")
        return report  # no point continuing

    # 2) Required tables present
    tables = {r[0] for r in fetch_all(engine, SQL_TABLES)}
    missing = sorted(list(REQUIRED_TABLES - tables))
    report.add(
        "schema_tables",
        len(missing) == 0,
        details=("all required tables present" if not missing else f"missing: {', '.join(missing)}"),
        data=sorted(list(tables)),
    )

    # 3) Alembic version present
    try:
        head = fetch_one(engine, SQL_ALEMBIC_HEAD)
        report.add("alembic_head", True, f"version={head}")
    except Exception as e:
        report.add("alembic_head", False, f"failed to read alembic_version: {e}")

    # 4) Row counts snapshot
    counts = [dict(table=r[0], count=int(r[1])) for r in fetch_all(engine, SQL_COUNTS)]
    report.add("row_counts", True, data=counts)

    # 5) Max dates & recency
    max_dates = {r[0]: r[1] for r in fetch_all(engine, SQL_MAX_DATES)}
    today = dt.date.today()
    # prices
    prices_ok = True
    prices_detail = "ok"
    if max_dates.get("prices"):
        lag = (today - max_dates["prices"]).days
        prices_ok = lag <= PRICES_MAX_LAG_DAYS
        prices_detail = f"max(prices.date)={max_dates['prices']} lag={lag}d (threshold {PRICES_MAX_LAG_DAYS}d)"
    else:
        prices_ok = False
        prices_detail = "no prices found"
    report.add("prices_recency", prices_ok, prices_detail)

    # signals
    signals_ok = True
    signals_detail = "ok"
    if max_dates.get("signals"):
        lag = (today - max_dates["signals"]).days
        signals_ok = lag <= SIGNALS_MAX_LAG_DAYS
        signals_detail = f"max(signals.date)={max_dates['signals']} lag={lag}d (threshold {SIGNALS_MAX_LAG_DAYS}d)"
    else:
        signals_ok = False
        signals_detail = "no signals found"
    report.add("signals_recency", signals_ok, signals_detail)

    # 6) Duplicates
    dup_prices = [dict(ticker=r[0], date=str(r[1]), count=int(r[2])) for r in fetch_all(engine, SQL_DUP_PRICES)]
    dup_signals = [dict(ticker=r[0], date=str(r[1]), count=int(r[2])) for r in fetch_all(engine, SQL_DUP_SIGNALS)]
    report.add("duplicates_prices", len(dup_prices) == 0, details="none" if not dup_prices else "found", data=dup_prices[:20])
    report.add("duplicates_signals", len(dup_signals) == 0, details="none" if not dup_signals else "found", data=dup_signals[:20])

    # 7) Orphans vs tickers
    orphan_prices = [dict(ticker=r[0], count=int(r[1])) for r in fetch_all(engine, SQL_ORPHAN_PRICES)]
    orphan_signals = [dict(ticker=r[0], count=int(r[1])) for r in fetch_all(engine, SQL_ORPHAN_SIGNALS)]
    report.add("orphans_prices", len(orphan_prices) == 0, details="none" if not orphan_prices else "found", data=orphan_prices[:20])
    report.add("orphans_signals", len(orphan_signals) == 0, details="none" if not orphan_signals else "found", data=orphan_signals[:20])

    # 8) Index definitions & JSON rules
    defs = [dict(slug=r[0], name=r[1], rules=r[2]) for r in fetch_all(engine, SQL_INDEX_DEFS)]
    slugs_present = {d["slug"] for d in defs}
    missing_slugs = sorted(list(REQUIRED_INDEX_SLUGS - slugs_present))
    # Validate rules look like JSON
    bad_rules = []
    for d in defs:
        try:
            if d["rules"] is None:
                bad_rules.append({"slug": d["slug"], "issue": "rules NULL"})
            else:
                json.loads(d["rules"])
        except Exception as e:
            bad_rules.append({"slug": d["slug"], "issue": f"invalid JSON: {e}"})
    ok_defs = len(missing_slugs) == 0 and len(bad_rules) == 0
    details_defs = []
    if missing_slugs:
        details_defs.append(f"missing slugs: {', '.join(missing_slugs)}")
    if bad_rules:
        details_defs.append(f"bad rules: {len(bad_rules)}")
    report.add("index_definitions", ok_defs, "; ".join(details_defs) or "ok", data={"present": sorted(list(slugs_present)), "bad_rules": bad_rules})

    # 9) Constituent weights sum ~ 1.0
    sums = [dict(slug=r[0], asof=str(r[1]), weight_sum=float(r[2] or 0.0), n=int(r[3])) for r in fetch_all(engine, SQL_CONS_WEIGHT_SUMS)]
    bad_sums = []
    for row in sums:
        if not (1.0 - WEIGHT_TOLERANCE) <= row["weight_sum"] <= (1.0 + WEIGHT_TOLERANCE):
            bad_sums.append(row)
    report.add("constituent_weight_sums", len(bad_sums) == 0, details=("ok" if not bad_sums else "out of bounds"), data=bad_sums[:20])

    # 10) Index history presence & freshness
    hist = [dict(slug=r[0], max_date=str(r[1]), rows=int(r[2])) for r in fetch_all(engine, SQL_INDEX_HISTORY_RECENT)]
    hist_ok = True
    hist_details = []
    for h in hist:
        try:
            d = dt.date.fromisoformat(h["max_date"])
            if (today - d).days > PRICES_MAX_LAG_DAYS + 2:
                hist_ok = False
                hist_details.append(f"{h['slug']} stale (max_date {h['max_date']})")
        except Exception:
            hist_ok = False
            hist_details.append(f"{h['slug']} invalid max_date={h['max_date']}")
    if not hist:
        hist_ok = False
        hist_details.append("no index_history rows")
    report.add("index_history_freshness", hist_ok, "; ".join(hist_details) or "ok", data=hist[:10])

    # 11) Tiny sample read
    sample = [dict(ticker=r[0], date=str(r[1]), close=float(r[2])) for r in fetch_all(engine, SQL_SAMPLE_PRICES)]
    report.add("sample_prices", len(sample) > 0, "ok" if sample else "no rows", data=sample)

    return report

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set in environment.", file=sys.stderr)
        sys.exit(1)

    engine = create_engine(db_url, pool_pre_ping=True, future=True)

    report = do_audit(engine)

    # pretty console summary
    print("\n=== Finance DB Audit Report ===")
    print(f"Started:   {report.started_at}")
    print(f"Database:  {report.database_url_redacted}")
    print("--------------------------------")
    width = 34
    for r in report.results:
        status = "PASS" if r.ok else "FAIL"
        print(f"{r.name.ljust(width,'.')}{status}")
        if r.details:
            print(f"  - {r.details}")
    print("--------------------------------")
    print(f"OVERALL: {'PASS' if report.ok else 'FAIL'}")

    # JSON output (machine-readable)
    if os.getenv("AUDIT_JSON", "0") in ("1", "true", "True"):
        print(json.dumps(report.to_dict(), indent=2))

    sys.exit(0 if report.ok else 1)

if __name__ == "__main__":
    main()

