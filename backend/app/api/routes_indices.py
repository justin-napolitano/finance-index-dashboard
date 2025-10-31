from fastapi import APIRouter, HTTPException
from typing import List, Dict
from sqlalchemy import text
from app.models.db import engine

router = APIRouter()

@router.get("/")
def list_indices():
    with engine.connect() as conn:
        res = conn.execute(text("SELECT id, slug, name, description FROM index_definitions ORDER BY id"))
        return [dict(r._mapping) for r in res]

@router.get("/{slug}")
def get_index(slug: str):
    with engine.connect() as conn:
        idx = conn.execute(text("SELECT id, slug, name, description FROM index_definitions WHERE slug=:s"), {"s": slug}).mappings().first()
        if not idx:
            raise HTTPException(404, "index not found")
        hist = conn.execute(text("SELECT date, level FROM index_history WHERE index_id=:i ORDER BY date"), {"i": idx["id"]}).mappings().all()
        holds = conn.execute(text("SELECT ticker, weight FROM index_constituents WHERE index_id=:i ORDER BY weight DESC"), {"i": idx["id"]}).mappings().all()
        return {"meta": idx, "history": hist, "holdings": holds}
