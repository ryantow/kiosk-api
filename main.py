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

def normalize_session_id(s: str) -> str:
    return (s or "").replace("-", "").upper()

class RestartClickPayload(BaseModel):
    session_id: str

# -----------------------------
# Config & helpers
# -----------------------------
def resolve_db_url() -> str:
    internal = os.environ.get("DATABASE_URL_INTERNAL")
    public = os.environ.get("DATABASE_URL_PUBLIC")
    fallback = os.environ.get("DATABASE_URL")
    url = internal or public or fallback
    if not url:
        raise RuntimeError("No database URL found.")
    return url

DATABASE_URL = resolve_db_url()
API_KEY = os.environ.get("API_KEY")

def require_api_key(authorization: Optional[str] = Header(None)) -> None:
    if not API_KEY:
        return 
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=0,
    max_size=5,
    kwargs={"row_factory": dict_row, "autocommit": True},
)

# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Kiosk Sessions API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Models
# -----------------------------
class StartSessionIn(BaseModel):
    kiosk_id: str
    session_id: str  # <--- Now required from Unreal
    app_version: Optional[str] = None

class CompleteSessionIn(BaseModel):
    session_id: str
    kiosk_id: str    # <--- Added so the server knows where to assign offline data
    client_ms: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None

class AbandonSessionIn(BaseModel):
    session_id: str
    kiosk_id: str    # <--- Added so the server knows where to assign offline data
    client_ms: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None

class RestartSessionIn(BaseModel):
    session_id: str

# -----------------------------
# Routes (The Upserts)
# -----------------------------
@app.post("/session/start", dependencies=[Depends(require_api_key)])
def start_session(payload: StartSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        # Check if kiosk exists
        cur.execute("SELECT 1 FROM kiosk_locations WHERE kiosk_id = %s;", (payload.kiosk_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=400, detail="Unknown kiosk_id")

        # Insert, or do nothing if the GUID already made it
        cur.execute(
            """
            INSERT INTO sessions (session_id, kiosk_id, app_version)
            VALUES (%s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
            RETURNING session_id, started_at;
            """,
            (payload.session_id, payload.kiosk_id, payload.app_version),
        )
        conn.commit()
        return {"ok": True, "session_id": payload.session_id}

@app.post("/session/complete", dependencies=[Depends(require_api_key)])
def complete_session(payload: CompleteSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        # The Upsert: Update if exists, Insert if offline Start was missed
        cur.execute(
            """
            INSERT INTO sessions (session_id, kiosk_id, completed_at, client_ms, meta)
            VALUES (%s, %s, NOW(), %s, %s)
            ON CONFLICT (session_id) DO UPDATE 
            SET completed_at = EXCLUDED.completed_at,
                client_ms    = COALESCE(EXCLUDED.client_ms, sessions.client_ms),
                meta         = COALESCE(EXCLUDED.meta, sessions.meta)
            RETURNING session_id;
            """,
            (payload.session_id, payload.kiosk_id, payload.client_ms, json.dumps(payload.meta) if payload.meta else None),
        )
        conn.commit()
        return {"ok": True, "session_id": payload.session_id}

@app.post("/session/abandon", dependencies=[Depends(require_api_key)])
def abandon_session(payload: AbandonSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        # The Upsert: Update if exists, Insert if offline Start was missed
        cur.execute(
            """
            INSERT INTO sessions (session_id, kiosk_id, abandoned_at, client_ms, meta)
            VALUES (%s, %s, NOW(), %s, %s)
            ON CONFLICT (session_id) DO UPDATE 
            SET abandoned_at = EXCLUDED.abandoned_at,
                client_ms    = COALESCE(EXCLUDED.client_ms, sessions.client_ms),
                meta         = COALESCE(EXCLUDED.meta, sessions.meta)
            RETURNING session_id;
            """,
            (payload.session_id, payload.kiosk_id, payload.client_ms, json.dumps(payload.meta) if payload.meta else None),
        )
        conn.commit()
        return {"ok": True, "session_id": payload.session_id}

@app.post("/session/restart", dependencies=[Depends(require_api_key)])
def restart_session(payload: RestartSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        # Increment the restart counter for this specific session
        cur.execute(
            """
            UPDATE sessions 
            SET restart_clicks = COALESCE(restart_clicks, 0) + 1
            WHERE session_id = %s
            RETURNING session_id;
            """,
            (payload.session_id,)
        )
        conn.commit()
        return {"ok": True, "session_id": payload.session_id}

# ------ Kiosks endpoint ------
@app.get("/kiosks", dependencies=[Depends(require_api_key)])
def get_kiosks():
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT kiosk_id, kiosk_name FROM kiosk_locations ORDER BY kiosk_name;")
        return cur.fetchall()


# ----- Metrics -----
@app.get("/metrics/overview", dependencies=[Depends(require_api_key)])
def metrics_overview(
    kiosk_id: Optional[str] = Query(default=None),
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
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
            COUNT(*) AS sessions_started,
            COUNT(*) FILTER (WHERE completed_at IS NOT NULL) AS sessions_completed,
            COUNT(*) FILTER (WHERE abandoned_at IS NOT NULL) AS sessions_abandoned,
            SUM(COALESCE(restart_clicks,0)) AS restart_clicks,
            AVG(COALESCE(client_ms, EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)) FILTER (WHERE completed_at IS NOT NULL) AS avg_completed_ms,
            AVG(COALESCE(client_ms, EXTRACT(EPOCH FROM (abandoned_at - started_at)) * 1000)) FILTER (WHERE abandoned_at IS NOT NULL) AS avg_abandoned_ms,
            SUM((meta->>'download_app_clicks')::numeric) AS download_app_clicks,
            SUM((meta->>'click_location_clicks')::numeric) AS click_location_clicks,
            SUM((meta->>'back_to_map_clicks')::numeric) AS back_to_map_sessions,
            AVG((meta->>'easter_eggs')::numeric) AS avg_easter_eggs,
            SUM((meta->'poi_clicks'->>'Priority Pass')::numeric) AS poi_1,
            SUM((meta->'poi_clicks'->>'Barcode Booth')::numeric) AS poi_2,
            SUM((meta->'poi_clicks'->>'Support Spotlight')::numeric) AS poi_3,
            SUM((meta->'poi_clicks'->>'Self Service Station')::numeric) AS poi_4,
            SUM((meta->'poi_clicks'->>'Cash Concession')::numeric) AS poi_5
        FROM base;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_from, date_to, date_to, kiosk_id, kiosk_id))
        row = cur.fetchone() or {}

    sessions_completed = int(row.get("sessions_completed", 0) or 0)
    restart_clicks = int(row.get("restart_clicks", 0) or 0)
    restart_rate = (float(restart_clicks) / float(sessions_completed)) if sessions_completed else 0.0

    poi_clicks = {
        "Priority Pass": int(row.get("poi_1") or 0),
        "Barcode Booth": int(row.get("poi_2") or 0),
        "Support Spotlight": int(row.get("poi_3") or 0),
        "Self Service Station": int(row.get("poi_4") or 0),
        "Cash Concession": int(row.get("poi_5") or 0),
    }

    return {
        "scope": kiosk_id,
        "date_from": date_from,
        "date_to": date_to,
        "sessions_started": int(row.get("sessions_started", 0) or 0),
        "sessions_completed": sessions_completed,
        "sessions_abandoned": int(row.get("sessions_abandoned", 0) or 0),
        "restart_clicks": restart_clicks,
        "restart_rate": restart_rate,
        "avg_completed_ms": float(row.get("avg_completed_ms")) if row.get("avg_completed_ms") else None,
        "avg_abandoned_ms": float(row.get("avg_abandoned_ms")) if row.get("avg_abandoned_ms") else None,
        "download_app_clicks": int(row.get("download_app_clicks") or 0),
        "click_location_clicks": int(row.get("click_location_clicks") or 0),
        "back_to_map_sessions": int(row.get("back_to_map_sessions") or 0),
        "avg_easter_eggs": float(row.get("avg_easter_eggs")) if row.get("avg_easter_eggs") else None,
        "poi_clicks": poi_clicks
    }

@app.get("/metrics/by-kiosk", dependencies=[Depends(require_api_key)])
def metrics_by_kiosk(
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
):
    sql = """
        SELECT
            kiosk_id,
            COUNT(*) AS started,
            COUNT(*) FILTER (WHERE completed_at IS NOT NULL) AS completed,
            COUNT(*) FILTER (WHERE abandoned_at IS NOT NULL) AS abandoned,
            SUM(COALESCE(restart_clicks,0)) AS restart_clicks,
            AVG(COALESCE(client_ms, EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)) FILTER (WHERE completed_at IS NOT NULL) AS avg_completed_ms,
            AVG(COALESCE(client_ms, EXTRACT(EPOCH FROM (abandoned_at - started_at)) * 1000)) FILTER (WHERE abandoned_at IS NOT NULL) AS avg_abandoned_ms,
            SUM((meta->>'download_app_clicks')::numeric) AS download_app_clicks,
            SUM((meta->>'click_location_clicks')::numeric) AS click_location_clicks,
            SUM((meta->>'back_to_map_clicks')::numeric) AS back_to_map_sessions,
            AVG((meta->>'easter_eggs')::numeric) AS avg_easter_eggs,
            AVG((meta->>'average_depth')::numeric) AS avg_screen_depth,
            SUM((meta->'poi_clicks'->>'Priority Pass')::numeric) AS poi_1,
            SUM((meta->'poi_clicks'->>'Barcode Booth')::numeric) AS poi_2,
            SUM((meta->'poi_clicks'->>'Support Spotlight')::numeric) AS poi_3,
            SUM((meta->'poi_clicks'->>'Self Service Station')::numeric) AS poi_4,
            SUM((meta->'poi_clicks'->>'Cash Concession')::numeric) AS poi_5
        FROM sessions
        WHERE (%s::timestamptz IS NULL OR started_at >= %s::timestamptz)
          AND (%s::timestamptz IS NULL OR started_at <  %s::timestamptz)
        GROUP BY kiosk_id
        ORDER BY kiosk_id;
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (date_from, date_from, date_to, date_to))
        rows = cur.fetchall()
        
        result.append({
                "kiosk_id": r.get("kiosk_id"),
                "started": int(r.get("started", 0) or 0),           # Fixed label
                "completed": sessions_completed,                    # Fixed label
                "abandoned": int(r.get("abandoned", 0) or 0),       # Fixed label
                "restart_clicks": restart_clicks,
                "restart_rate": restart_rate,
                "avg_completed_ms": float(r.get("avg_completed_ms")) if r.get("avg_completed_ms") else None,
                "avg_abandoned_ms": float(r.get("avg_abandoned_ms")) if r.get("avg_abandoned_ms") else None,
                "download_app_clicks": int(r.get("download_app_clicks") or 0),
                "click_location_clicks": int(r.get("click_location_clicks") or 0),
                "back_to_map_sessions": int(r.get("back_to_map_sessions") or 0),
                "avg_easter_eggs": float(r.get("avg_easter_eggs")) if r.get("avg_easter_eggs") else None,
                "avg_abandoned_screen_depth": float(r.get("avg_screen_depth")) if r.get("avg_screen_depth") else None, # Fixed label
                "poi_clicks": {
                    "Priority Pass": int(r.get("poi_1") or 0),
                    "Barcode Booth": int(r.get("poi_2") or 0),
                    "Support Spotlight": int(r.get("poi_3") or 0),
                    "Self Service Station": int(r.get("poi_4") or 0),
                    "Cash Concession": int(r.get("poi_5") or 0),
                }
            })
            
        return result

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)