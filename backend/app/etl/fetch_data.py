# backend/app/etl/fetch_data.py
import os
import time
import random
import datetime as dt
from typing import List, Iterable, Optional
import traceback

import pandas as pd
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text
from app.models.db import engine
from sqlalchemy import text, bindparam, String
from sqlalchemy.dialects.postgresql import ARRAY, VARCHAR
# -------- Environment defaults --------
os.environ.setdefault("YFINANCE_USE_CURL", "true")
os.environ.setdefault("CURL_CA_BUNDLE", "/usr/local/lib/python3.11/site-packages/certifi/cacert.pem")

# -------- Tunables (env-driven) --------
YF_MAX_BATCH        = int(os.getenv("YF_MAX_BATCH", "25"))
YF_SLEEP_SEC        = float(os.getenv("YF_SLEEP_SEC", "1.5"))
YF_MAX_RETRIES      = int(os.getenv("YF_MAX_RETRIES", "6"))
YF_BACKOFF_FACTOR   = float(os.getenv("YF_BACKOFF_FACTOR", "1.5"))
YF_ADAPTIVE_SLOWSEC = float(os.getenv("YF_ADAPTIVE_SLOWSEC", "6.0"))
YF_THREADS          = os.getenv("YF_THREADS", "false").lower() in ("1","true","yes")
YF_PERIOD_DAYS      = int(os.getenv("YF_PERIOD_DAYS", "365"))

# -------- Shared session with robust retries --------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=YF_MAX_RETRIES,
        backoff_factor=YF_BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = build_session()

# -------- Token-bucket rate limiter --------
_last_call_ts = 0.0
_extra_slow_until = 0.0

def _rate_sleep():
    global _last_call_ts, _extra_slow_until
    now = time.time()
    delay = YF_SLEEP_SEC
    if now < _extra_slow_until:
        delay = max(delay, YF_ADAPTIVE_SLOWSEC)
    since = now - _last_call_ts
    need = delay - since
    if need > 0:
        time.sleep(need + random.uniform(0, 0.4))
    _last_call_ts = time.time()

def _mark_slowdown(seconds: float = 60.0):
    global _extra_slow_until
    _extra_slow_until = max(_extra_slow_until, time.time() + seconds)

# -------- Helpers --------
def _get_max_price_date() -> Optional[dt.date]:
    with engine.begin() as conn:
        r = conn.execute(text("SELECT MAX(date)::date FROM prices")).scalar()
        return r

def _chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# -------- Main download with full tracebacks --------
def _download_with_backoff(tickers: List[str], start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    _rate_sleep()
    try:
        print(f"[fetch] calling yf.download: batch={len(tickers)} tickers={tickers} window={start}->{end}", flush=True)
        df = yf.download(
            tickers=" ".join(tickers),
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            actions=False,
            group_by="ticker",
            progress=False,
            threads=YF_THREADS,
            #session=SESSION,
            timeout=30,
        )
        print(f"[fetch] yf.download returned shape={getattr(df,'shape',None)}", flush=True)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        msg = str(e).lower()
        print(f"[fetch][ERROR] yf.download failed for {tickers}: {e}\n{traceback.format_exc()}", flush=True)
        if "429" in msg or "too many requests" in msg:
            _mark_slowdown(180.0)
        raise

# -------- Normalize yf.download output --------
#def _tidy_prices(batch_df: pd.DataFrame, batch_tickers: List[str]) -> pd.DataFrame:
#    if batch_df.empty:
#        return pd.DataFrame(columns=["ticker","date","open","high","low","close","volume"])
#    if isinstance(batch_df.columns, pd.MultiIndex):
#        pieces = []
#        for tkr in batch_tickers:
#            if tkr not in batch_df.columns.get_level_values(1):
#                continue
#            sub = batch_df.xs(tkr, axis=1, level=1, drop_level=False)
#            sub = sub.droplevel(1, axis=1)
#            sub = sub.rename(columns=str.lower)
#            sub = sub.reset_index().rename(columns={"index":"date"})
#            sub["ticker"] = tkr
#            pieces.append(sub[["ticker","date","open","high","low","close","volume"]])
#        return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame(columns=["ticker","date","open","high","low","close","volume"])
#    else:
#        df1 = batch_df.rename(columns=str.lower).reset_index().rename(columns={"index":"date"})
#        tkr = batch_tickers[0]
#        df1["ticker"] = tkr
#        return df1[["ticker","date","open","high","low","close","volume"]]

def _tidy_prices(batch_df: pd.DataFrame, batch_tickers: List[str]) -> pd.DataFrame:
    cols = ["ticker","date","open","high","low","close","volume"]
    if batch_df.empty:
        return pd.DataFrame(columns=cols)

    # Single-index (single ticker) path
    if not isinstance(batch_df.columns, pd.MultiIndex):
        df1 = batch_df.rename(columns=str.lower).reset_index().rename(columns={"index":"date"})
        if "close" not in df1 and "adj close" in df1:
            df1["close"] = df1["adj close"]
        df1["ticker"] = batch_tickers[0]
        out = df1[[c for c in cols if c in df1.columns]]
        return out.dropna(subset=[c for c in ("close","volume") if c in out.columns], how="all")

    # ---- MultiIndex path ----
    fields = {"Open","High","Low","Close","Adj Close","Volume"}
    # Detect level order and normalize to (ticker, field)
    lvl0 = {t[0] for t in batch_df.columns[:min(10, len(batch_df.columns))]}
    if any(x in fields for x in lvl0):  # currently (field, ticker)
        df = batch_df.swaplevel(0,1,axis=1)
    else:                                # already (ticker, field)
        df = batch_df

    # Enforce names
    df.columns = pd.MultiIndex.from_tuples(df.columns, names=["ticker","field"])

    # Stack to long
    long = (
        df.stack(level="ticker")
          .rename_axis(index=["date","ticker"])
          .reset_index()
    )
    long.columns = [c.lower() for c in long.columns]
    if "close" not in long and "adj close" in long:
        long["close"] = long["adj close"]

    out = long[[c for c in cols if c in long.columns]]
    return out.dropna(subset=[c for c in ("close","volume") if c in out.columns], how="all")


# -------- Main pipeline --------
def fetch_prices(tickers: List[str]):
    print(f"[fetch] starting fetch_prices: {len(tickers)} tickers", flush=True)
    today = dt.date.today()
    max_existing = _get_max_price_date()
    start = (today - dt.timedelta(days=YF_PERIOD_DAYS)).isoformat() if max_existing is None \
        else (max_existing + dt.timedelta(days=1)).isoformat()
    end = (today + dt.timedelta(days=1)).isoformat()
    print(f"[fetch] date window: {start} → {end}", flush=True)

    if not tickers:
        print("[fetch] no tickers provided; abort", flush=True)
        return

    all_rows = []
    for batch in _chunked(tickers, YF_MAX_BATCH):
        print(f"[fetch] downloading batch size={len(batch)} -> {batch}", flush=True)
        try:
            df_raw = _download_with_backoff(batch, start=start, end=end)
            tidy = _tidy_prices(df_raw, batch)
            print(f"[fetch] tidy rows for batch={len(tidy)}", flush=True)
            all_rows.append(tidy)
        except Exception as e:
            print(f"[fetch][WARN] batch failed; splitting: {e}\n{traceback.format_exc()}", flush=True)
            _mark_slowdown(120.0)
            if len(batch) > 1:
                mid = len(batch)//2
                for sub in (batch[:mid], batch[mid:]):
                    try:
                        print(f"[fetch] retry sub-batch {sub}", flush=True)
                        df_sub = _download_with_backoff(sub, start=start, end=end)
                        tidy = _tidy_prices(df_sub, sub)
                        print(f"[fetch] tidy rows sub-batch={len(tidy)}", flush=True)
                        all_rows.append(tidy)
                    except Exception as e2:
                        print(f"[fetch][WARN] sub-batch failed; trying singles: {e2}\n{traceback.format_exc()}", flush=True)
                        for t in sub:
                            try:
                                print(f"[fetch] retry single -> {t}", flush=True)
                                df_single = _download_with_backoff([t], start=start, end=end)
                                tidy = _tidy_prices(df_single, [t])
                                print(f"[fetch] tidy rows single={len(tidy)} ({t})", flush=True)
                                all_rows.append(tidy)
                            except Exception as e3:
                                print(f"[fetch][ERR] single failed {t}: {e3}\n{traceback.format_exc()}", flush=True)
                                time.sleep(YF_ADAPTIVE_SLOWSEC)
                                continue
            else:
                print("[fetch][WARN] single batch failed; skipping", flush=True)
                time.sleep(YF_ADAPTIVE_SLOWSEC)
                continue

    if not all_rows:
        print("[fetch] no rows downloaded; nothing to upsert", flush=True)
        return

    prices = pd.concat(all_rows, ignore_index=True)
    prices = prices.dropna(subset=["date", "close"])
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    print(f"[fetch] total rows after concat/clean: {len(prices)}", flush=True)

    try:
        with engine.begin() as conn:
            print("[fetch] ensuring tickers exist…", flush=True)
            tick_df = pd.DataFrame({"ticker": sorted(set(prices["ticker"]))})
            if not tick_df.empty:
                stmt = text("""
                    INSERT INTO tickers(ticker)
                    SELECT UNNEST(:tickers)
                    ON CONFLICT (ticker) DO NOTHING
                    """).bindparams(bindparam("tickers", type_=ARRAY(VARCHAR())))

                conn.execute(stmt, {"tickers": tickers})  # tickers: list[str]           

            #if not tick_df.empty:
            #    conn.execute(text("""
            #        INSERT INTO tickers(ticker)
            #        SELECT UNNEST(:tickers)
            #        ON CONFLICT (ticker) DO NOTHING
            #    """).bindparams(bindparam("tickers", type_=ARRAY(String()))))

            print("[fetch] staging tmp_prices…", flush=True)
            conn.execute(text("""
                CREATE TEMP TABLE IF NOT EXISTS tmp_prices(
                    ticker text, date date, open double precision, high double precision,
                    low double precision, close double precision, volume bigint
                ) ON COMMIT DROP
            """))

            chunk_size = 5000
            print(f"[fetch] bulk insert to tmp_prices in chunks of {chunk_size}", flush=True)
            for i in range(0, len(prices), chunk_size):
                sub = prices.iloc[i:i+chunk_size]
                records = sub[["ticker","date","open","high","low","close","volume"]].to_dict(orient="records")
                conn.execute(text("""
                    INSERT INTO tmp_prices(ticker,date,open,high,low,close,volume)
                    VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
                """), records)
                print(f"[fetch] inserted tmp chunk rows={len(records)} (offset {i})", flush=True)

            print("[fetch] upserting tmp -> prices…", flush=True)
            conn.execute(text("""
                INSERT INTO prices(ticker,date,open,high,low,close,volume)
                SELECT ticker,date,open,high,low,close,volume FROM tmp_prices
                ON CONFLICT (ticker, date) DO UPDATE SET
                  open=EXCLUDED.open,
                  high=EXCLUDED.high,
                  low=EXCLUDED.low,
                  close=EXCLUDED.close,
                  volume=EXCLUDED.volume
            """))
            print("[fetch] upsert complete.", flush=True)
    except Exception as e:
        print(f"[fetch][DB-ERROR] upsert failed: {e}\n{traceback.format_exc()}", flush=True)
        raise

