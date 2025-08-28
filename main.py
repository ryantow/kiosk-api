import os
import json
from datetime import datetime
from typing import Optional, Any, Dict, List
from io import StringIO
import csv

from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# -----------------------------
# Config & helpers
# -----------------------------
def resolve_db_url() -> str:
    """
    Prefer internal when present (Railway), else public (local), else DATABASE_URL.
    """
    internal = os.environ.get("DATABASE_URL_INTERNAL")
    public = os.environ.get("DATABASE_URL_PUBLIC")
    fallback = os.environ.get("DATABASE_URL")
    url = internal or public or fallback
    if not url:
        raise RuntimeError("No database URL found. Set DATABASE_URL_INTERNAL or DATABASE_URL_PUBLIC or DATABASE_URL.")
    return url

DATABASE_URL = resolve_db_url()
API_KEY = os.environ.get("API_KEY")

def require_api_key(authorization: Optional[str] = Header(None)) -> None:
    """
    Simple bearer token check. Set API_KEY env on Railway and your dashboard proxy will attach it.
    /health remains open.
    """
    if not API_KEY:
        return  # allow unauth in dev if API_KEY not set
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

# Lazy pool (no connections opened until first query)
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=0,
    max_size=5,
    kwargs={"row_factory": dict_row},
)

# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Kiosk Sessions API", version="1.1.0")

# CORS (loose; tighten for production domains if desired)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Models
# -----------------------------
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

class RestartSessionIn(BaseModel):
    session_id: str

class KioskRow(BaseModel):
    kiosk_id: str
    kiosk_name: str

class ByKioskRow(BaseModel):
    kiosk_id: str
    started: int
    completed: int
    abandoned: int
    restart_clicks: int
    avg_ms: Optional[float]

class MetricsOverviewOut(BaseModel):
    scope: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    sessions_started: int
    sessions_completed: int
    sessions_abandoned: int
    restart_clicks: int
    restart_rate: float
    avg_session_ms: Optional[float]

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    # DB-independent health endpoint
    return {"ok": True, "version": app.version}

@app.get("/db/ping", dependencies=[Depends(require_api_key)])
def db_ping():
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT now() AS ts;")
        row = cur.fetchone()
        return {"ok": True, "ts": row["ts"].isoformat()}

@app.get("/kiosks", response_model=List[KioskRow], dependencies=[Depends(require_api_key)])
def list_kiosks(only_active: bool = True):
    sql = """
        SELECT kiosk_id, kiosk_name
        FROM kiosk_locations
        WHERE (%s::bool IS FALSE) OR (is_active = TRUE)
        ORDER BY kiosk_name;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (only_active,))
        return cur.fetchall()

# ----- Sessions -----
@app.post("/session/start", response_model=StartSessionOut, dependencies=[Depends(require_api_key)])
def start_session(payload: StartSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        # validate kiosk
        cur.execute("SELECT 1 FROM kiosk_locations WHERE kiosk_id = %s;", (payload.kiosk_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=400, detail="Unknown kiosk_id")

        cur.execute(
            """
            INSERT INTO sessions (kiosk_id, app_version)
            VALUES (%s, %s)
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
                client_ms   = COALESCE(%s, client_ms),
                meta        = COALESCE(%s, meta)
            WHERE session_id = %s
            RETURNING session_id;
            """,
            (payload.client_ms, json.dumps(payload.meta) if payload.meta else None, payload.session_id),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="session_id not found")
        return {"ok": True, "session_id": payload.session_id}

@app.post("/session/abandon", dependencies=[Depends(require_api_key)])
def abandon_session(payload: AbandonSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE sessions SET abandoned_at = NOW() WHERE session_id = %s RETURNING session_id;",
            (payload.session_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="session_id not found")
        return {"ok": True, "session_id": payload.session_id}

@app.post("/session/restart", dependencies=[Depends(require_api_key)])
def restart_session(payload: RestartSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sessions
            SET restart_clicks = COALESCE(restart_clicks, 0) + 1
            WHERE session_id = %s
            RETURNING restart_clicks;
            """,
            (payload.session_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="session_id not found")
        return {"ok": True, "session_id": payload.session_id, "restart_clicks": int(row["restart_clicks"])}

# ----- Metrics: JSON -----
@app.get("/metrics/overview", response_model=MetricsOverviewOut, dependencies=[Depends(require_api_key)])
def metrics_overview(
    kiosk_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None, description="ISO date e.g. 2025-08-01"),
    date_to:   Optional[str] = Query(default=None, description="ISO date (exclusive) e.g. 2025-09-01"),
):
    sql = """
        WITH base AS (
            SELECT *
            FROM sessions
            WHERE (%s::timestamptz IS NULL OR started_at >= %s::timestamptz)
              AND (%s::timestamptz IS NULL OR started_at <  %s::timestamptz)
              AND (%s::text IS NULL OR kiosk_id = %s)
        )
        SELECT
            COUNT(*)                                                    AS sessions_started,
            COUNT(*) FILTER (WHERE completed_at IS NOT NULL)            AS sessions_completed,
            COUNT(*) FILTER (WHERE abandoned_at IS NOT NULL)            AS sessions_abandoned,
            SUM(COALESCE(restart_clicks,0))                             AS restart_clicks,
            AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, abandoned_at) - started_at))) * 1000
              AS avg_session_ms
        FROM base;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_from, date_to, date_to, kiosk_id, kiosk_id))
        row = cur.fetchone() or {}

    sessions_completed = int(row.get("sessions_completed", 0) or 0)
    restart_clicks = int(row.get("restart_clicks", 0) or 0)
    restart_rate = (float(restart_clicks) / float(sessions_completed)) if sessions_completed else 0.0

    return {
        "scope": kiosk_id,
        "date_from": date_from,
        "date_to": date_to,
        "sessions_started": int(row.get("sessions_started", 0) or 0),
        "sessions_completed": sessions_completed,
        "sessions_abandoned": int(row.get("sessions_abandoned", 0) or 0),
        "restart_clicks": restart_clicks,
        "restart_rate": restart_rate,
        "avg_session_ms": float(row.get("avg_session_ms")) if row.get("avg_session_ms") is not None else None,
    }

@app.get("/metrics/by-kiosk", response_model=List[ByKioskRow], dependencies=[Depends(require_api_key)])
def metrics_by_kiosk(
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
):
    sql = """
        SELECT
            kiosk_id,
            COUNT(*)                                                    AS started,
            COUNT(*) FILTER (WHERE completed_at IS NOT NULL)            AS completed,
            COUNT(*) FILTER (WHERE abandoned_at IS NOT NULL)            AS abandoned,
            SUM(COALESCE(restart_clicks,0))                             AS restart_clicks,
            AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, abandoned_at) - started_at))) * 1000
                AS avg_ms
        FROM sessions
        WHERE (%s::timestamptz IS NULL OR started_at >= %s::timestamptz)
          AND (%s::timestamptz IS NULL OR started_at <  %s::timestamptz)
        GROUP BY kiosk_id
        ORDER BY kiosk_id;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_from, date_to, date_to))
        rows = cur.fetchall()
        return rows

# ----- Metrics: CSV -----
@app.get("/metrics/by-kiosk.csv", dependencies=[Depends(require_api_key)])
def metrics_by_kiosk_csv(
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
):
    sql = """
        SELECT
            s.kiosk_id,
            k.kiosk_name,
            COUNT(*)                                                    AS started,
            COUNT(*) FILTER (WHERE s.completed_at IS NOT NULL)          AS completed,
            COUNT(*) FILTER (WHERE s.abandoned_at IS NOT NULL)          AS abandoned,
            SUM(COALESCE(s.restart_clicks,0))                           AS restart_clicks,
            AVG(EXTRACT(EPOCH FROM (COALESCE(s.completed_at, s.abandoned_at) - s.started_at)))
              AS avg_sec
        FROM sessions s
        JOIN kiosk_locations k ON k.kiosk_id = s.kiosk_id
        WHERE (%s::timestamptz IS NULL OR s.started_at >= %s::timestamptz)
          AND (%s::timestamptz IS NULL OR s.started_at <  %s::timestamptz)
        GROUP BY s.kiosk_id, k.kiosk_name
        ORDER BY k.kiosk_name;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_from, date_to, date_to))
        rows = cur.fetchall()

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "kiosk_id","kiosk_name","started","completed","abandoned",
        "restart_clicks","restart_rate","avg_seconds"
    ])
    for r in rows:
        started   = int(r["started"] or 0)
        completed = int(r["completed"] or 0)
        abandoned = int(r["abandoned"] or 0)
        rc        = int(r["restart_clicks"] or 0)
        rate      = (rc / completed) if completed else 0.0
        avg_sec   = r["avg_sec"] if r["avg_sec"] is not None else ""
        avg_fmt   = f"{avg_sec:.1f}" if avg_sec != "" else ""
        writer.writerow([
            r["kiosk_id"], r["kiosk_name"], started, completed, abandoned,
            rc, f"{rate:.4f}", avg_fmt
        ])

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="metrics_by_kiosk.csv"'},
    )

# -----------------------------
# Local entrypoint (Railway sets $PORT)
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)