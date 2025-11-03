from datetime import date as _date
from sqlalchemy import text
from psycopg.types.json import Json  # <-- lets psycopg3 adapt Python dict -> JSON/JSONB
from app.models.db import engine
import datetime as dt


def ensure_default_index():
    rules = {
        "universe": "US",
        "select": "top",
        "by": "m_score",
        "n": 10,
        "caps": {"sector": 0.35, "ticker": 0.2},
        "floors": {"dollar_vol": 0},
        "weight": "equal",
    }
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO index_definitions(slug, name, description, rules, rebalance_freq, reconst_freq)
                VALUES('momentum-10','Momentum 10','Top 10 by simple momentum score', :rules, 'monthly', 'monthly')
                ON CONFLICT (slug) DO NOTHING
            """),
            {"rules": Json(rules)},  # <-- wrap dict so it binds as JSON
        )


def reconstitute_and_rebalance(asof: _date | None = None):
    ensure_default_index()
    if asof is None:
        asof = dt.date.today()

    with engine.begin() as conn:
        # fetch index id (after ensure)
        idx = conn.execute(
            text("SELECT id FROM index_definitions WHERE slug='momentum-10'")
        ).mappings().first()
        if not idx:
            # nothing to do if index isn't present for some reason
            return
        index_id = idx["id"]

        # pick latest signals per ticker
        sigs = conn.execute(
            text("""
                WITH s AS (
                  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                  FROM signals s
                )
                SELECT ticker, m_score
                FROM s
                WHERE rn = 1
                ORDER BY m_score DESC
                LIMIT 10
            """)
        ).mappings().all()

        if not sigs:
            # no signals yet; skip today
            return

        # reset constituents for 'asof' and insert equal weights
        weight = 1.0 / len(sigs)
        conn.execute(
            text("DELETE FROM index_constituents WHERE index_id=:i AND asof=:d"),
            {"i": index_id, "d": asof},
        )
        for r in sigs:
            conn.execute(
                text("""
                    INSERT INTO index_constituents(index_id, asof, ticker, weight)
                    VALUES(:i, :d, :t, :w)
                """),
                {"i": index_id, "d": asof, "t": r["ticker"], "w": weight},
            )

        # compute today's daily return from constituents
        prev = conn.execute(
            text("""
                SELECT date, level
                FROM index_history
                WHERE index_id = :i
                ORDER BY date DESC
                LIMIT 1
            """),
            {"i": index_id},
        ).mappings().first()
        level_prev = float(prev["level"]) if prev else 1000.0

        rets = conn.execute(
            text("""
                WITH p AS (
                  SELECT ticker, date, close,
                         LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
                  FROM prices
                  WHERE date <= :d
                )
                SELECT AVG((close - prev_close)/NULLIF(prev_close,0)) AS ret
                FROM p
                WHERE date = :d
                  AND prev_close IS NOT NULL
                  AND ticker IN (
                      SELECT ticker
                      FROM index_constituents
                      WHERE index_id = :i AND asof = :d
                  )
            """),
            {"d": asof, "i": index_id},
        ).scalar()

        ret_daily = float(rets or 0.0)
        level = level_prev * (1.0 + ret_daily)

        conn.execute(
            text("""
                INSERT INTO index_history(index_id, date, level, ret_daily)
                VALUES(:i, :d, :l, :r)
                ON CONFLICT (index_id, date)
                DO UPDATE SET level = EXCLUDED.level, ret_daily = EXCLUDED.ret_daily
            """),
            {"i": index_id, "d": asof, "l": level, "r": ret_daily},
        )


if __name__ == "__main__":
    reconstitute_and_rebalance()

