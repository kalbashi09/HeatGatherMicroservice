import io
from datetime import date
import httpx
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from apscheduler.schedulers.background import BackgroundScheduler

# ==========================================
# 1. CONFIGURATION
# ==========================================
DB_HOST = "aws-1-ap-southeast-2.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"
DB_USER = "postgres.ymszyrnklqnohvjfyutr"
DB_PASS = "heatsync0728"

SENSOR_API_URL = "https://refactorheatalertsqlite.onrender.com/api/Alerts/history"
SYNC_TOKEN = "thesis-2026-secret" 

# ==========================================
# 2. APP SETUP & DB INITIALIZATION
# ==========================================
app = FastAPI(title="Thesis Data Collector")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

# ==========================================
# 3. CORE SYNC LOGIC
# ==========================================
def run_sync_logic():
    print("⏳ [Background] Fetching latest data from external API...")
    try:
        response = httpx.get(SENSOR_API_URL, timeout=30)
        response.raise_for_status()
        logs = response.json().get("data", {}).get("logs", [])
    except Exception as e:
        print(f"❌ [Background] Failed to fetch external API: {e}")
        return

    rows_to_insert = []
    for log in logs:
        if log.get("sensorCode", "").startswith("TEST"):
            rows_to_insert.append((
                log.get("id"), 
                log.get("sensorCode"), 
                log.get("heatIndex"),
                log.get("humidity"), 
                log.get("recordedAt")
            ))

    if not rows_to_insert:
        print("✅ [Background] Sync complete. No new TEST sensors found.")
        return

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO test_sensor_readings 
                        (api_id, sensor_code, heat_index, humidity, recorded_at)
                    VALUES %s
                    ON CONFLICT (api_id) DO NOTHING
                    """,
                    rows_to_insert
                )
            conn.commit()
        print(f"✅ [Background] Sync complete. Ingested {len(rows_to_insert)} rows.")
    except Exception as e:
        print(f"❌ [Background] Database error: {e}")

# ==========================================
# 4. STARTUP EVENT & SCHEDULER
# ==========================================
@app.on_event("startup")
def startup_event():
    # 1. Create table if it doesn't exist
    print("Connecting to Supabase to check tables...")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS test_sensor_readings (
                        api_id BIGINT PRIMARY KEY,
                        sensor_code TEXT NOT NULL,
                        heat_index INT NOT NULL,
                        humidity INT NOT NULL,
                        recorded_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_recorded_at ON test_sensor_readings (recorded_at);
                """)
            conn.commit()
        print("✅ Table initialized successfully!")
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")

    # 2. Start the internal background loop
    scheduler = BackgroundScheduler()
    # Runs every 1 minute. (You can change this to minutes=5 if preferred)
    scheduler.add_job(run_sync_logic, 'interval', minutes=1)
    scheduler.start()
    print("🔄 Background auto-sync started! Running every 1 minute.")

# ==========================================
# 5. ENDPOINTS
# ==========================================

@app.get("/health")
def health_check():
    """Ping this endpoint via cron-job.org to keep Render awake."""
    return {"status": "awake"}

@app.post("/sync")
def manual_sync(x_sync_token: str = Header(...)):
    """Optional: Allows you to manually trigger a sync if needed."""
    if x_sync_token != SYNC_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid sync token")
    
    run_sync_logic()
    return {"status": "success", "message": "Manual sync triggered. Check server logs."}

@app.get("/export.xlsx")
def export_excel(
    start: date = Query(..., description="Start date YYYY-MM-DD"),
    end: date = Query(..., description="End date YYYY-MM-DD")
):
    query = """
        SELECT 
            sensor_code, 
            heat_index, 
            humidity, 
            recorded_at AT TIME ZONE 'Asia/Manila' AS recorded_pht
        FROM test_sensor_readings
        WHERE (recorded_at AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        ORDER BY recorded_at ASC
    """
    
    try:
        with get_db() as conn:
            df = pd.read_sql_query(query, conn, params=(start, end))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected date range")

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All Data", index=False)
        for sensor_code, group in df.groupby("sensor_code"):
            safe_sheet_name = str(sensor_code)[:31]
            group.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            
    buffer.seek(0)
    filename = f"readings_{start}_to_{end}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )