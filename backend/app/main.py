from fastapi import FastAPI
from app.models.db import SessionLocal
from app.api.routes_indices import router as indices_router
from app.api.routes_tickers import router as tickers_router

app = FastAPI(title="Finance Index Dashboard API")

@app.on_event("startup")
def startup_event():
    # Touch DB on startup to ensure connectivity
    db = SessionLocal()
    db.close()

app.include_router(indices_router, prefix="/indices", tags=["indices"])
app.include_router(tickers_router, prefix="/tickers", tags=["tickers"])

@app.get("/healthz")
def healthz():
    return {"status": "ok"}
