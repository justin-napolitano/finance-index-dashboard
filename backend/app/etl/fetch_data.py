import pandas as pd
import yfinance as yf
from sqlalchemy import text
from app.models.db import engine
from datetime import datetime, timedelta

UNIVERSE = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","AVGO","TSLA","BRK-B","AMD"]  # seed; replace w/ your list

def upsert_tickers():
    with engine.begin() as conn:
        for t in UNIVERSE:
            conn.execute(text("""
                INSERT INTO tickers(ticker, name, sector, exchange)
                VALUES(:t, :n, :s, :e)
                ON CONFLICT (ticker) DO NOTHING
            """), {"t": t, "n": t, "s": None, "e": None})

def fetch_prices(days: int = 400):
    upsert_tickers()
    tickers = " ".join(UNIVERSE)
    df = yf.download(tickers=tickers, period=f"{days}d", auto_adjust=True, group_by='ticker', progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        frames = []
        for t in UNIVERSE:
            try:
                sub = df[t][['Close','Volume']].dropna().rename(columns={'Close':'close','Volume':'volume'})
                sub['ticker'] = t
                frames.append(sub)
            except KeyError:
                continue
        prices = pd.concat(frames).reset_index().rename(columns={'Date':'date'})
    else:
        prices = df[['Close','Volume']].dropna().rename(columns={'Close':'close','Volume':'volume'}).reset_index()
        prices['ticker'] = UNIVERSE[0]
        prices = prices.rename(columns={'Date':'date'})
    with engine.begin() as conn:
        for _, r in prices.iterrows():
            conn.execute(text("""
                INSERT INTO prices(ticker, date, close, volume)
                VALUES(:ticker, :date, :close, :volume)
                ON CONFLICT (ticker, date) DO UPDATE SET close=EXCLUDED.close, volume=EXCLUDED.volume
            """), dict(ticker=r['ticker'], date=r['date'].date(), close=float(r['close']), volume=int(r['volume'])))

if __name__ == "__main__":
    fetch_prices()
