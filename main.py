import os
import json
from datetime import datetime
from typing import Optional, Any, Dict, List

from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg
from psycopg_pool import ConnectionPool 
from psycopg.rows import dict_row

# --- Config ---
DATABASE_URL = os.environ.get("DATABASE_URL")
API_KEY = os.environ.get("API_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is required")

# Connection pool (psycopg3)
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=5,
    kwargs={"row_factory": dict_row}  # pass row_factory through kwargs
)
app = FastAPI(title="Kiosk Sessions API", version="1.0.0")

# (Optional) allow local dev UIs
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Simple API key auth ---
def require_api_key(authorization: Optional[str] = Header(None)) -> None:
    if not API_KEY:
        return  # if you want unauth during local dev, leave API_KEY unset
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# --- Pydantic models ---
class StartSessionIn(BaseModel):
    kiosk_id: str = Field(..., description="e.g., KIOSK-59LEX")
    app_version: Optional[str] = None

class StartSessionOut(BaseModel):
    session_id: str
    started_at: datetime

class CompleteSessionIn(BaseModel):
    session_id: str
    client_ms: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None

class AbandonSessionIn(BaseModel):
    session_id: str

class MetricsOverviewOut(BaseModel):
    scope: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    sessions_started: int
    sessions_completed: int
    sessions_abandoned: int
    avg_session_ms: Optional[float]

class KioskRow(BaseModel):
    kiosk_id: str
    kiosk_name: str

class ByKioskRow(BaseModel):
    kiosk_id: str
    started: int
    completed: int
    abandoned: int
    avg_ms: Optional[float]

# --- Routes ---

@app.get("/health")
def health():
    return {"ok": True, "version": app.version}

@app.get("/kiosks", response_model=List[KioskRow], dependencies=[Depends(require_api_key)])
def list_kiosks(only_active: bool = True):
    sql = """
        SELECT kiosk_id, kiosk_name
        FROM kiosk_locations
        WHERE ($1::bool IS FALSE) OR (is_active = TRUE)
        ORDER BY kiosk_name;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (only_active,))
        rows = cur.fetchall()
        return rows

@app.post("/session/start", response_model=StartSessionOut, dependencies=[Depends(require_api_key)])
def start_session(payload: StartSessionIn):
    # Ensure kiosk exists
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM kiosk_locations WHERE kiosk_id = $1", (payload.kiosk_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=400, detail="Unknown kiosk_id")

        cur.execute(
            """
            INSERT INTO sessions (kiosk_id, app_version)
            VALUES ($1, $2)
            RETURNING session_id, started_at;
            """,
            (payload.kiosk_id, payload.app_version),
        )
        row = cur.fetchone()
        return {"session_id": str(row["session_id"]), "started_at": row["started_at"]}

@app.post("/session/complete", dependencies=[Depends(require_api_key)])
def complete_session(payload: CompleteSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sessions
            SET completed_at = NOW(),
                client_ms = COALESCE($2, client_ms),
                meta = COALESCE($3, meta)
            WHERE session_id = $1
            RETURNING session_id;
            """,
            (payload.session_id, payload.client_ms, json.dumps(payload.meta) if payload.meta else None),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="session_id not found")
        return {"ok": True, "session_id": payload.session_id}

@app.post("/session/abandon", dependencies=[Depends(require_api_key)])
def abandon_session(payload: AbandonSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sessions
            SET abandoned_at = NOW()
            WHERE session_id = $1
            RETURNING session_id;
            """,
            (payload.session_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="session_id not found")
        return {"ok": True, "session_id": payload.session_id}

@app.get("/metrics/overview", response_model=MetricsOverviewOut, dependencies=[Depends(require_api_key)])
def metrics_overview(
    kiosk_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None, description="ISO date e.g. 2025-08-01"),
    date_to: Optional[str] = Query(default=None, description="ISO date (exclusive) e.g. 2025-09-01"),
):
    # Defaults: last 30 days if not provided
    sql = """
        WITH base AS (
            SELECT *
            FROM sessions
            WHERE ($1::timestamp IS NULL OR started_at >= $1::timestamp)
              AND ($2::timestamp IS NULL OR started_at <  $2::timestamp)
              AND ($3::text IS NULL OR kiosk_id = $3)
        )
        SELECT
            COUNT(*)                                          AS sessions_started,
            COUNT(*) FILTER (WHERE completed)                 AS sessions_completed,
            COUNT(*) FILTER (WHERE abandoned_at IS NOT NULL)  AS sessions_abandoned,
            AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, abandoned_at) - started_at))) * 1000
                AS avg_session_ms
        FROM base;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_to, kiosk_id))
        row = cur.fetchone() or {}
        return {
            "scope": kiosk_id,
            "date_from": date_from,
            "date_to": date_to,
            "sessions_started": int(row.get("sessions_started", 0) or 0),
            "sessions_completed": int(row.get("sessions_completed", 0) or 0),
            "sessions_abandoned": int(row.get("sessions_abandoned", 0) or 0),
            "avg_session_ms": float(row.get("avg_session_ms")) if row.get("avg_session_ms") is not None else None,
        }

@app.get("/metrics/by-kiosk", response_model=List[ByKioskRow], dependencies=[Depends(require_api_key)])
def metrics_by_kiosk(
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
):
    sql = """
        SELECT
            kiosk_id,
            COUNT(*)                                         AS started,
            COUNT(*) FILTER (WHERE completed)                AS completed,
            COUNT(*) FILTER (WHERE abandoned_at IS NOT NULL) AS abandoned,
            AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, abandoned_at) - started_at))) * 1000
                AS avg_ms
        FROM sessions
        WHERE ($1::timestamp IS NULL OR started_at >= $1::timestamp)
          AND ($2::timestamp IS NULL OR started_at <  $2::timestamp)
        GROUP BY kiosk_id
        ORDER BY kiosk_id;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_to))
        rows = cur.fetchall()
        return rows

# --- Local entrypoint (Railway uses PORT) ---
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)