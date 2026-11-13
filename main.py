import io
import math
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
# 3. NOAA FORMULA (TRANSLATED FROM C#)
# ==========================================
def calculate_heat_index(temp_celsius: float, humidity: float) -> float:
    # LAYER 1: INPUT SAFETY CLAMPS
    temp_celsius = max(10.0, min(temp_celsius, 45.0))
    humidity = max(0.0, min(humidity, 100.0))

    # Convert Celsius to Fahrenheit
    T = (temp_celsius * 9.0 / 5.0) + 32.0
    RH = humidity

    # STEP 1: SIMPLIFIED STEADMAN FORMULA
    HI_simple = 0.5 * (T + 61.0 + ((T - 68.0) * 1.2) + (RH * 0.094))

    # STEP 2: AVERAGE WITH ACTUAL TEMPERATURE
    HI = (HI_simple + T) / 2.0

    # STEP 3: APPLY ROTHFUSZ ONLY IF INITIAL HI >= 80°F
    if HI >= 80.0:
        HI = -42.379 + \
             (2.04901523 * T) + \
             (10.14333127 * RH) - \
             (0.22475541 * T * RH) - \
             (0.00683783 * T * T) - \
             (0.05481717 * RH * RH) + \
             (0.00122874 * T * T * RH) + \
             (0.00085282 * T * RH * RH) - \
             (0.00000199 * T * T * RH * RH)

        # LOW HUMIDITY ADJUSTMENT
        if RH < 13.0 and 80.0 <= T <= 112.0:
            adjustment = ((13.0 - RH) / 4.0) * math.sqrt((17.0 - abs(T - 95.0)) / 17.0)
            HI -= adjustment
        # HIGH HUMIDITY ADJUSTMENT
        elif RH > 85.0 and 80.0 <= T <= 87.0:
            adjustment = ((RH - 85.0) / 10.0) * ((87.0 - T) / 5.0)
            HI += adjustment

    # STEP 4: CONVERT BACK TO CELSIUS
    heat_index_celsius = (HI - 32.0) * 5.0 / 9.0

    # LAYER 2: APPLICATION-SPECIFIC OUTPUT SAFETY CLAMP
    if heat_index_celsius > 70.0:
        heat_index_celsius = 70.0

    return heat_index_celsius

# ==========================================
# 4. REVERSE CALCULATION (BINARY SEARCH)
# ==========================================
def calculate_ambient_temp(heat_index_celsius: float, humidity: float) -> float:
    """
    Uses Binary Search to find the ambient temperature that produces the given Heat Index.
    Because the formula clamps temp between 10 and 45, we search within that range.
    """
    low = 10.0
    high = 45.0
    
    # 40 iterations is extremely precise (converges to fractions of a degree)
    for _ in range(40):
        mid = (low + high) / 2.0
        calculated_hi = calculate_heat_index(mid, humidity)
        
        # If the calculated HI is lower than the target HI, we need a higher temp
        if calculated_hi < heat_index_celsius:
            low = mid
        else:
            high = mid
            
    return round((low + high) / 2.0, 1)

# ==========================================
# 5. CORE SYNC LOGIC
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
        if log.get("sensorCode", "").startswith("SENSOR-PROTOTYPE"):
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
# 6. STARTUP EVENT & SCHEDULER
# ==========================================
@app.on_event("startup")
def startup_event():
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

    scheduler = BackgroundScheduler()
    scheduler.add_job(run_sync_logic, 'interval', minutes=1)
    scheduler.start()
    print("🔄 Background auto-sync started! Running every 1 minute.")

# ==========================================
# 7. ENDPOINTS
# ==========================================
@app.get("/health")
def health_check():
    return {"status": "awake"}

@app.post("/sync")
def manual_sync(x_sync_token: str = Header(...)):
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

    # --- NEW: REVERSE CALCULATION ---
    # Apply the reverse math to every row and create a new column
    df['calculated_ambient_c'] = df.apply(
        lambda row: calculate_ambient_temp(row['heat_index'], row['humidity']), 
        axis=1
    )

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        
        # 1. Original Data Sheet
        df_original = df.drop(columns=['calculated_ambient_c'])
        df_original.to_excel(writer, sheet_name="Original Data", index=False)
        
        # 2. New Reversed Ambient Sheet
        df.to_excel(writer, sheet_name="Reversed Ambient Temp", index=False)
        
        # 3. Per-sensor Sheets (includes the new reversed column)
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