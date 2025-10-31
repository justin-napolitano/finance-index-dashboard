from sqlalchemy import text
from app.models.db import engine
import datetime as dt

def ensure_default_index():
    rules = {"universe":"US", "select":"top", "by":"m_score", "n":10, "caps":{"sector":0.35,"ticker":0.2}, "floors":{"dollar_vol":0}, "weight":"equal"}
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO index_definitions(slug, name, description, rules, rebalance_freq, reconst_freq)
            VALUES('momentum-10','Momentum 10','Top 10 by simple momentum score', :rules, 'monthly', 'monthly')
            ON CONFLICT (slug) DO NOTHING
        """), {"rules": rules})

def reconstitute_and_rebalance(asof=None):
    ensure_default_index()
    if asof is None:
        asof = dt.date.today()
    with engine.begin() as conn:
        idx = conn.execute(text("SELECT id, rules FROM index_definitions WHERE slug='momentum-10'")).mappings().first()
        # pick latest signals per ticker
        sigs = conn.execute(text("""
            WITH s AS (
              SELECT s.*, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
              FROM signals s
            )
            SELECT ticker, m_score FROM s WHERE rn=1 ORDER BY m_score DESC LIMIT 10
        """)).mappings().all()
        if not sigs:
            return
        weight = 1.0/len(sigs)
        conn.execute(text("DELETE FROM index_constituents WHERE index_id=:i AND asof=:d"), {"i": idx["id"], "d": asof})
        for r in sigs:
            conn.execute(text("""
                INSERT INTO index_constituents(index_id, asof, ticker, weight)
                VALUES(:i, :d, :t, :w)
            """), {"i": idx["id"], "d": asof, "t": r["ticker"], "w": weight})

        # compute today's return and index level
        # init level 1000 if first day
        prev = conn.execute(text("SELECT date, level FROM index_history WHERE index_id=:i ORDER BY date DESC LIMIT 1"), {"i": idx["id"]}).mappings().first()
        level_prev = prev["level"] if prev else 1000.0
        # approximate daily ret as avg of constituents' latest daily return
        rets = conn.execute(text("""
            WITH p AS (
              SELECT ticker, date, close,
                     LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
              FROM prices WHERE date <= :d
            )
            SELECT AVG((close - prev_close)/NULLIF(prev_close,0)) AS ret
            FROM p WHERE date = :d AND prev_close IS NOT NULL AND ticker IN (
                SELECT ticker FROM index_constituents WHERE index_id=:i AND asof=:d
            )
        """), {"d": asof, "i": idx["id"]}).scalar()
        ret_daily = float(rets or 0.0)
        level = level_prev * (1.0 + ret_daily)
        conn.execute(text("""
            INSERT INTO index_history(index_id, date, level, ret_daily)
            VALUES(:i, :d, :l, :r)
            ON CONFLICT (index_id,date) DO UPDATE SET level=EXCLUDED.level, ret_daily=EXCLUDED.ret_daily
        """), {"i": idx["id"], "d": asof, "l": level, "r": ret_daily})

if __name__ == "__main__":
    reconstitute_and_rebalance()
