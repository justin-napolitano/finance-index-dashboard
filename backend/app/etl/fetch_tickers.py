from sqlalchemy import create_engine, text
from typing import List, Optional

def load_tickers_from_db(db_url: str, universe: Optional[str]=None, limit: Optional[int]=None) -> List[str]:
    eng = create_engine(db_url, future=True)
    sql = """
    SELECT ticker
    FROM tickers
    WHERE coalesce(is_active, true)
      AND (:universe IS NULL OR (:universe = ANY(universe)))
    ORDER BY ticker
    """
    if limit:
        sql += " LIMIT :limit"
    with eng.begin() as conn:
        rows = conn.execute(text(sql), {"universe": universe, "limit": limit}).scalars().all()
    return rows

