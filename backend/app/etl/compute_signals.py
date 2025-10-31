import pandas as pd
import numpy as np
from sqlalchemy import text
from app.models.db import engine

def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1*delta.clip(upper=0)
    ma_up = up.ewm(com=period-1, adjust=False).mean()
    ma_down = down.ewm(com=period-1, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def compute_all_signals():
    with engine.begin() as conn:
        tickers = [r[0] for r in conn.execute(text("SELECT ticker FROM tickers"))]
        for t in tickers:
            df = pd.read_sql_query(text("SELECT date, close, volume FROM prices WHERE ticker=:t ORDER BY date"), conn.connection, params={"t": t})
            if df.empty or len(df) < 60:
                continue
            df['ret'] = df['close'].pct_change()
            df['ret_1m'] = df['close'].pct_change(21)
            df['ret_3m'] = df['close'].pct_change(63)
            df['ret_6m'] = df['close'].pct_change(126)
            df['sma50'] = df['close'].rolling(50).mean()
            df['sma200'] = df['close'].rolling(200).mean()
            df['atr_14'] = df['close'].pct_change().abs().rolling(14).mean() * df['close']
            df['rsi_14'] = rsi(df['close'], 14)
            df['vol_surge'] = df['volume'] / df['volume'].rolling(20).mean()
            df['m_score'] = (df['ret_3m'].rank(pct=True) * 0.6 + df['ret_6m'].rank(pct=True) * 0.4) - (df['atr_14'].rank(pct=True) * 0.2)
            df['breakout'] = (df['close'] > df['sma50']) & (df['sma50'] > df['sma200']) & (df['rsi_14'] > 60) & (df['vol_surge'] > 1.2)
            out = df[['date','ret_1m','ret_3m','ret_6m','rsi_14','atr_14','sma50','sma200','vol_surge','m_score','breakout']].dropna()
            for _, r in out.iterrows():
                conn.execute(text("""
                    INSERT INTO signals(ticker, date, ret_1m, ret_3m, ret_6m, rsi_14, atr_14, sma50, sma200, vol_surge, m_score, breakout)
                    VALUES(:t, :d, :r1, :r3, :r6, :rsi, :atr, :s50, :s200, :vs, :ms, :bo)
                    ON CONFLICT (ticker,date) DO UPDATE SET
                      ret_1m=EXCLUDED.ret_1m, ret_3m=EXCLUDED.ret_3m, ret_6m=EXCLUDED.ret_6m,
                      rsi_14=EXCLUDED.rsi_14, atr_14=EXCLUDED.atr_14,
                      sma50=EXCLUDED.sma50, sma200=EXCLUDED.sma200,
                      vol_surge=EXCLUDED.vol_surge, m_score=EXCLUDED.m_score, breakout=EXCLUDED.breakout
                """), dict(t=t, d=r['date'], r1=float(r['ret_1m']), r3=float(r['ret_3m']), r6=float(r['ret_6m']),
                             rsi=float(r['rsi_14']), atr=float(r['atr_14']), s50=float(r['sma50']), s200=float(r['sma200']),
                             vs=float(r['vol_surge']), ms=float(r['m_score']), bo=bool(r['breakout'])))

if __name__ == "__main__":
    compute_all_signals()
