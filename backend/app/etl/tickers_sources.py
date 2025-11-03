# -*- coding: utf-8 -*-
"""
Fetch S&P 500 and NASDAQ-100 tickers, normalize for yfinance,
export to YAML, and optionally upsert into DB.
"""
import argparse
import os
import sys
import pandas as pd
import yaml
import requests
import io


URL_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
URL_NDQ100 = "https://en.wikipedia.org/wiki/Nasdaq-100"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Python-Requests/2.x"}

def _yf_symbol(s: str) -> str:
    s = s.strip().upper()
    if "." in s:
        base, suf = s.split(".", 1)
        if len(suf) <= 3:
            return f"{base}-{suf}"
    return s

#def _get_tables(url: str):
#    r = requests.get(url, headers=UA, timeout=20)
#    r.raise_for_status()
#    return pd.read_html(r.text, flavor="lxml")

def _get_tables(url: str):
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    return pd.read_html(io.StringIO(r.text), flavor="lxml")

def _cols_lower(df):
    return [str(c).strip().lower() for c in df.columns]


def fetch_sp500() -> pd.DataFrame:
    try:
        tables = _get_tables(URL_SP500)
        df = tables[0]
    except Exception:
        # Fallback (community CSV; may lag): symbol column "Symbol"
        df = pd.read_csv(
            "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        )
    sym_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
    out = (df[[sym_col]].rename(columns={sym_col: "symbol"}))
    out["ticker"] = out["symbol"].astype(str).map(_yf_symbol)
    return out[["ticker"]].drop_duplicates().reset_index(drop=True)


def fetch_nasdaq100() -> pd.DataFrame:
    # pick the table that contains a ticker column (case/TYPE agnostic)
    tables = _get_tables(URL_NDQ100)
    tgt = None
    for t in tables:
        cols = _cols_lower(t)
        if any(x in cols for x in ["ticker", "symbol"]):
            tgt = t
            break
    if tgt is None:
        # fallback via Wikipedia API HTML
        api = "https://en.wikipedia.org/w/api.php"
        params = dict(action="parse", page="Nasdaq-100", prop="text", format="json", formatversion=2)
        r = requests.get(api, params=params, headers=UA, timeout=20); r.raise_for_status()
        html = r.json()["parse"]["text"]
        tables = pd.read_html(io.StringIO(html), flavor="lxml")
        for t in tables:
            cols = _cols_lower(t)
            if any(x in cols for x in ["ticker", "symbol"]):
                tgt = t
                break
        if tgt is None:
            raise RuntimeError("NASDAQ-100 constituents table not found")

    cols = _cols_lower(tgt)
    # choose the ticker column by name (string-insensitive)
    if "ticker" in cols:
        col = tgt.columns[cols.index("ticker")]
    elif "symbol" in cols:
        col = tgt.columns[cols.index("symbol")]
    else:
        col = tgt.columns[0]  # last resort

    out = (tgt[[col]]
           .rename(columns={col: "symbol"})
           .assign(ticker=lambda d: d["symbol"].astype(str).map(_yf_symbol)))
    return out[["ticker"]].drop_duplicates().reset_index(drop=True)
#def fetch_nasdaq100() -> pd.DataFrame:
#    try:
#        tables = _get_tables(URL_NDQ100)
#    except Exception:
#        # Fallback (Wikipedia API -> HTML)
#        api = "https://en.wikipedia.org/w/api.php"
#        params = dict(action="parse", page="Nasdaq-100", prop="text", format="json", formatversion=2)
#        r = requests.get(api, params=params, headers=UA, timeout=20); r.raise_for_status()
#        html = r.json()["parse"]["text"]
#        tables = pd.read_html(html, flavor="lxml")
#
#    tgt = next((t for t in tables if any(c.lower() in ["ticker","symbol"] for c in t.columns)), None)
#    if tgt is None:
#        raise RuntimeError("NASDAQ-100 table not found")
#    col = "Ticker" if "Ticker" in tgt.columns else ("Symbol" if "Symbol" in tgt.columns else list(tgt.columns)[0])
#    out = tgt[[col]].rename(columns={col: "symbol"})
#    out["ticker"] = out["symbol"].astype(str).map(_yf_symbol)
#    return out[["ticker"]].drop_duplicates().reset_index(drop=True)





#def fetch_sp500() -> pd.DataFrame:
 #   df = pd.read_html(URL_SP500, flavor="lxml")[0]
 #   sym_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
 #   df = df[[sym_col]].rename(columns={sym_col: "symbol"})
#    df["ticker"] = df["symbol"].astype(str).map(_yf_symbol)
#    return df[["ticker"]].drop_duplicates()



#def fetch_nasdaq100() -> pd.DataFrame:
  #  tables = pd.read_html(URL_NDQ100, flavor="lxml")
 #   tgt = next(
 #       (t for t in tables if any(c.lower() in ["ticker", "symbol"] for c in t.columns)),
 #       None
 #   )
 #   if tgt is None:
 #       raise RuntimeError("No NASDAQ-100 table found")
#    col = "Ticker" if "Ticker" in tgt.columns else ("Symbol" if "Symbol" in tgt.columns else list(tgt.columns)[0])
#    df = tgt[[col]].rename(columns={col: "symbol"})
#    df["ticker"] = df["symbol"].astype(str).map(_yf_symbol)
#    return df[["ticker"]].drop_duplicates()

def write_yaml(path: str, sp: pd.DataFrame, ndq: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {
        "sp500": sorted(sp["ticker"].tolist()),
        "nasdaq100": sorted(ndq["ticker"].tolist()),
        "us_core": sorted(set(sp["ticker"]).union(set(ndq["ticker"]))),
    }
    with open(path, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)
    print(f"Wrote YAML → {path} ({len(data['us_core'])} total tickers)")

def upsert_tickers(tickers, db_url: str) -> None:
    from sqlalchemy import create_engine, text, bindparam
    from sqlalchemy.dialects.postgresql import ARRAY, VARCHAR
    eng = create_engine(db_url, future=True)
    with eng.begin() as conn:
        stmt = text("""
            INSERT INTO tickers(ticker)
            SELECT UNNEST(:tickers)
            ON CONFLICT (ticker) DO NOTHING
        """).bindparams(bindparam("tickers", type_=ARRAY(VARCHAR())))
        conn.execute(stmt, {"tickers": tickers})
    print(f"Upserted {len(tickers)} tickers into DB")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--write", action="store_true", help="export tickers.yaml")
    p.add_argument("--db", action="store_true", help="upsert tickers into DB")
    p.add_argument("--db-url", default=os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@db:5432/finance"))
    args = p.parse_args()

    sp = fetch_sp500()
    ndq = fetch_nasdaq100()
    print(f"S&P 500: {len(sp)}, NASDAQ-100: {len(ndq)}")

    if args.write:
        write_yaml("/app/app/etl/tickers.yaml", sp, ndq)
    if args.db:
        all_tickers = sorted(set(sp["ticker"]).union(set(ndq["ticker"])))
        upsert_tickers(all_tickers, args.db_url)

if __name__ == "__main__":
    sys.exit(main())

