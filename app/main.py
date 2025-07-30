import os
import shutil
import psycopg2
import uuid
from datetime import datetime
from fastapi import FastAPI, File, UploadFile, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from redis import Redis
from rq import Queue
from psycopg2.extras import RealDictCursor

# --- App Configuration ---
app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# --- Environment Variables & Connections ---
# Railway provides these environment variables automatically
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/bulk_jobs_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

redis_conn = Redis.from_url(REDIS_URL)
q = Queue("high_priority", connection=redis_conn)

# --- Persistent Storage Setup ---
# On Railway, use a Volume for persistent file storage.
# We will assume the volume is mounted at /data
UPLOAD_FOLDER = "/data"
# Ensure the upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Database Functions ---
def get_db_connection():
    """Creates a connection to the PostgreSQL database."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def create_jobs_table():
    """Creates the jobs table in the database if it doesn't already exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            start_timestamp TIMESTAMPTZ NOT NULL,
            end_timestamp TIMESTAMPTZ,
            input_file_path TEXT NOT NULL,
            output_file_path TEXT,
            total_rows INTEGER,
            processed_rows INTEGER DEFAULT 0,
            error_message TEXT
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

# --- FastAPI Events ---
@app.on_event("startup")
def on_startup():
    """This function runs when the application starts."""
    create_jobs_table()

# --- FastAPI Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def get_jobs_list(request: Request):
    """Displays the main page with a list of all jobs."""
    conn = get_db_connection()
    # Use RealDictCursor to get results as dictionaries
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM jobs ORDER BY start_timestamp DESC")
    jobs = cursor.fetchall()
    cursor.close()
    conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})

@app.post("/upload", response_class=RedirectResponse)
async def create_upload_file(file: UploadFile = File(...)):
    """Handles file upload, creates a job, and enqueues it."""
    job_id = str(uuid.uuid4())
    
    original_filename = file.filename
    input_path = os.path.join(UPLOAD_FOLDER, f"{job_id}_{original_filename}")
    with open(input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    with open(input_path, 'r', errors='ignore') as f:
        total_rows = sum(1 for line in f) - 1

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO jobs (job_id, status, start_timestamp, input_file_path, total_rows)
           VALUES (%s, %s, %s, %s, %s)""",
        (job_id, "pending", datetime.utcnow(), input_path, total_rows)
    )
    conn.commit()
    cursor.close()
    conn.close()

    output_path = os.path.join(UPLOAD_FOLDER, f"{job_id}_output.csv")
    q.enqueue(
        'worker.processing_script.run_bulk_job', 
        job_id=job_id, 
        input_file_path=input_path, 
        output_file_path=output_path,
        job_timeout=36000 # Allow jobs to run for up to 10 hours
    )

    return RedirectResponse(url="/", status_code=303)

@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """API endpoint to get the status of a specific job."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))
    job = cursor.fetchone()
    cursor.close()
    conn.close()
    if job:
        return dict(job)
    return {"error": "Job not found"}

@app.get("/data/{filename}")
async def download_file(filename: str):
    """Serves a file from the persistent data volume."""
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename, media_type='text/csv')
    return {"error": "File not found"}