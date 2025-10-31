from fastapi import APIRouter
from sqlalchemy import text
from app.models.db import engine

router = APIRouter()

@router.get("/{ticker}")
def get_ticker(ticker: str):
    with engine.connect() as conn:
        info = conn.execute(text("SELECT * FROM tickers WHERE ticker=:t"), {"t": ticker.upper()}).mappings().first()
        prices = conn.execute(text("SELECT date, close, volume FROM prices WHERE ticker=:t ORDER BY date DESC LIMIT 100"), {"t": ticker.upper()}).mappings().all()
        return {"info": info, "prices": prices}
