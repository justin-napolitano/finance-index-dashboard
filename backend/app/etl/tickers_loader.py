# backend/app/etl/tickers_loader.py
from __future__ import annotations
import os, csv, re
from typing import List, Set
from pathlib import Path

try:
    import yaml  # optional, only needed for .yaml/.yml
except Exception:
    yaml = None

SYMBOL_RE = re.compile(r"^[A-Za-z0-9\.\-\_]+$")  # allow dots (BRK.B), dashes, etc.

def _clean(s: str) -> str:
    s = s.strip().upper()
    return s

def _is_symbol(s: str) -> bool:
    return bool(SYMBOL_RE.match(s))

def _from_txt(p: Path) -> List[str]:
    out: List[str] = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        # allow trailing comments: "AAPL  # apple"
        line = line.split("#", 1)[0].strip()
        sym = _clean(line)
        if sym and _is_symbol(sym):
            out.append(sym)
    return out

def _from_csv(p: Path) -> List[str]:
    out: List[str] = []
    with p.open(newline="") as f:
        rdr = csv.DictReader(f)
        col = None
        # try to find a likely column
        for candidate in ("ticker","symbol","Ticker","Symbol"):
            if candidate in rdr.fieldnames:
                col = candidate
                break
        if col is None:
            # no headers? fall back to first column via csv.reader
            f.seek(0)
            for row in csv.reader(f):
                if not row:
                    continue
                sym = _clean(row[0])
                if sym and _is_symbol(sym):
                    out.append(sym)
            return out
        for row in rdr:
            sym = _clean(row[col])
            if sym and _is_symbol(sym):
                out.append(sym)
    return out

def _from_yaml(p: Path) -> List[str]:
    if yaml is None:
        raise RuntimeError("PyYAML not installed; cannot read YAML tickers file.")
    data = yaml.safe_load(p.read_text()) or {}
    out: Set[str] = set()

    # support simple list:
    # tickers: [AAPL, MSFT, ...]
    if isinstance(data, dict) and "tickers" in data and isinstance(data["tickers"], list):
        for x in data["tickers"]:
            sym = _clean(str(x))
            if sym and _is_symbol(sym):
                out.add(sym)

    # support grouped config:
    # groups:
    #   tech: [AAPL, MSFT]
    #   energy: [XOM, CVX]
    # include: [tech]
    # exclude: [OTC]
    if isinstance(data, dict) and "groups" in data:
        groups = data.get("groups") or {}
        include = data.get("include") or []
        exclude = set(data.get("exclude") or [])
        # include listed groups
        for g in include:
            for x in groups.get(g, []):
                sym = _clean(str(x))
                if sym and _is_symbol(sym):
                    out.add(sym)
        # then drop excluded symbols
        out = {s for s in out if s not in exclude}

    # also allow raw list (top-level YAML sequence)
    if isinstance(data, list):
        for x in data:
            sym = _clean(str(x))
            if sym and _is_symbol(sym):
                out.add(sym)

    return sorted(out)

def load_tickers(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tickers file not found: {path}")
    ext = p.suffix.lower()
    if ext in (".txt",):
        syms = _from_txt(p)
    elif ext in (".csv",):
        syms = _from_csv(p)
    elif ext in (".yaml",".yml"):
        syms = _from_yaml(p)
    else:
        # fallback: try plain text
        syms = _from_txt(p)
    # dedupe & sort
    uniq = sorted({_clean(s) for s in syms if _is_symbol(s)})
    if not uniq:
        raise ValueError(f"No valid tickers found in {path}")
    return uniq

