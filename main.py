@app.post("/session/restart", dependencies=[Depends(require_api_key)])
def restart_session(payload: RestartSessionIn):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sessions
            SET restart_clicks = COALESCE(restart_clicks, 0) + 1
            WHERE session_id = %s
              AND completed_at IS NULL
              AND abandoned_at IS NULL
            RETURNING restart_clicks;
            """,
            (payload.session_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="session_id not found")
        return {"ok": True, "session_id": payload.session_id, "restart_clicks": int(row["restart_clicks"])}

@app.post("/session/restart_click", dependencies=[Depends(require_api_key)])
def restart_click(payload: RestartClickPayload):
    sid = normalize_session_id(payload.session_id)  # 32-char, no dashes, UPPER
    if not sid:
        raise HTTPException(status_code=400, detail="session_id required")

    sql = """
      UPDATE sessions
         SET restart_clicks = COALESCE(restart_clicks, 0) + 1
       WHERE REPLACE(UPPER(session_id::text), '-', '') = %s
         AND completed_at IS NULL
         AND abandoned_at IS NULL
       RETURNING restart_clicks;
    """

    # Use the same ConnectionPool pattern as other routes in this file
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (sid,))
        row = cur.fetchone()
        conn.commit()

    if row is None:
        raise HTTPException(status_code=404, detail="session not found")

    return {"ok": True, "restart_clicks": int(row["restart_clicks"]) }