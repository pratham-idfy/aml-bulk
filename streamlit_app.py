

import os
import streamlit as st
import pandas as pd
import psycopg2
import uuid
import threading
from datetime import datetime
import time
import shutil
import csv
import requests
from requests.exceptions import Timeout
from psycopg2.extras import RealDictCursor

# --- Page Configuration ---
st.set_page_config(
    page_title="Bulk Job Runner",
    layout="wide"
)

# --- Environment Variables & Connections ---
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

# --- Persistent Storage Setup ---
# On Railway, this path points to the attached Volume.
UPLOAD_FOLDER = "/data"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Database Functions ---
def get_db_connection():
    """Creates a connection to the PostgreSQL database."""
    if not DATABASE_URL:
        st.error("Database URL is not configured. Please set the DATABASE_URL environment variable.")
        return None
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    """Ensures the jobs table exists and reconciles stale jobs on startup."""
    conn = get_db_connection()
    if not conn:
        return
    with conn.cursor() as cursor:
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                job_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                start_timestamp TIMESTAMPTZ NOT NULL,
                end_timestamp TIMESTAMPTZ,
                input_filename TEXT NOT NULL,
                output_filename TEXT,
                total_rows INTEGER,
                processed_rows INTEGER DEFAULT 0,
                error_message TEXT
            );
        """)
        # Reconcile jobs that were 'running' during a restart
        cursor.execute("""
            UPDATE jobs
            SET status = 'failed', error_message = 'Application restarted during processing.'
            WHERE status = 'running';
        """)
    conn.commit()
    conn.close()

# --- API & Processing Logic (Adapted from processing_script.py) ---

def update_job_status(job_id, status, error_message=None):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE jobs SET status = %s, error_message = %s, end_timestamp = %s WHERE job_id = %s",
            (status, error_message, datetime.utcnow(), job_id)
        )
    conn.commit()
    conn.close()

def update_job_progress(job_id, processed_rows):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE jobs SET processed_rows = %s WHERE job_id = %s",
            (processed_rows, job_id)
        )
    conn.commit()
    conn.close()

def make_api_call(full_name, entity_type, dob=None, pan_number=None):
    headers = {
        'Content-Type': 'application/json',
        'api-key': API_KEY,
        'account-id': ACCOUNT_ID
    }
    filters = {
        "types": ["sanctions", "pep", "warnings"], "name_fuzziness": "1",
        "search_profile": "all_default", "country_codes": ["IN"],
        "entity_type": entity_type.lower()
    }
    if entity_type.lower() == 'individual' and dob and dob.strip():
        filters["birth_year"] = dob.strip()
    if pan_number and pan_number.strip():
        filters["pan_number"] = pan_number.strip()
    
    data = {
        "task_id": str(uuid.uuid4()), "group_id": str(uuid.uuid4()),
        "data": {
            "search_term": full_name, "filters": filters, "version": "2", "get_profile_pdf": False
        }
    }

    for _ in range(3): # 3 retries
        try:
            response = requests.post('https://api.idfy.com/v3/tasks/sync/verify_with_source/aml', headers=headers, json=data, timeout=120)
            response.raise_for_status()
            return response.json(), response.status_code, None
        except (Timeout, requests.exceptions.RequestException) as e:
            error_message = f"Error: {str(e)}"
            time.sleep(1)
    return None, -1, "Max retries reached"

def run_bulk_job(job_id, input_filepath, output_filepath):
    """The main function that runs in a background thread."""
    print(f"Starting job {job_id} for file {input_filepath}")
    update_job_status(job_id, "running")

    try:
        with open(input_filepath, mode='r', newline='', errors='ignore') as infile, \
             open(output_filepath, mode='w', newline='') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            try:
                header = next(reader)
                writer.writerow(header + ["api_status_code", "match_status", "total_hits", "api_error"])
            except StopIteration:
                raise ValueError("Input file is empty or invalid.")

            processed_count = 0
            for row in reader:
                if len(row) < 3: continue

                _, full_name, entity_type = row[0], row[1].strip(), row[2].strip()
                dob = row[3].strip() if len(row) > 3 else None
                pan_number = row[4].strip() if len(row) > 4 else None

                response_json, status_code, error = make_api_call(full_name, entity_type, dob, pan_number)

                match_status, total_hits = None, None
                if response_json and response_json.get("status") == "success":
                    result = response_json.get("result", {})
                    match_status = result.get("match_status")
                    total_hits = result.get("total_hits")
                
                writer.writerow(row + [status_code, match_status, total_hits, error])

                processed_count += 1
                if processed_count % 5 == 0:
                    update_job_progress(job_id, processed_count)
            
            update_job_progress(job_id, processed_count)

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE jobs SET output_filename = %s WHERE job_id = %s", (os.path.basename(output_filepath), job_id))
        conn.commit()
        conn.close()
        update_job_status(job_id, "completed")
        print(f"Job {job_id} completed successfully.")

    except Exception as e:
        print(f"Job {job_id} failed: {str(e)}")
        update_job_status(job_id, "failed", error_message=str(e))


# --- Streamlit UI ---
st.title("Bulk AML Check Runner")

# Perform setup once
setup_database()

st.header("Submit a New Job")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    if st.button("Start Processing"):
        job_id = str(uuid.uuid4())
        original_filename = uploaded_file.name
        
        # Save file to persistent volume
        input_filepath = os.path.join(UPLOAD_FOLDER, f"{job_id}_{original_filename}")
        with open(input_filepath, "wb") as f:
            f.write(uploaded_file.getbuffer())

        # Count rows
        with open(input_filepath, 'r', errors='ignore') as f:
            total_rows = sum(1 for line in f) - 1

        # Create DB entry
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO jobs (job_id, status, start_timestamp, input_filename, total_rows)
                   VALUES (%s, %s, %s, %s, %s)""",
                (job_id, "pending", datetime.utcnow(), original_filename, total_rows)
            )
        conn.commit()
        conn.close()

        # Start job in background thread
        output_filepath = os.path.join(UPLOAD_FOLDER, f"{job_id}_output.csv")
        thread = threading.Thread(target=run_bulk_job, args=(job_id, input_filepath, output_filepath))
        thread.start()

        st.success(f"Job submitted successfully! Job ID: {job_id}")
        st.info("Dashboard will update automatically.")
        time.sleep(2) # Give a moment for the page to auto-refresh
        st.experimental_rerun()


st.header("Job Dashboard")

# Fetch and display jobs
conn = get_db_connection()
if conn:
    jobs_df = pd.read_sql("SELECT * FROM jobs ORDER BY start_timestamp DESC", conn)
    conn.close()

    if not jobs_df.empty:
        # Custom styling for the table
        st.dataframe(
            jobs_df,
            column_config={
                "job_id": st.column_config.TextColumn("Job ID", help="Unique ID for the job"),
                "status": st.column_config.TextColumn("Status"),
                "start_timestamp": st.column_config.DatetimeColumn("Submitted At", format="YYYY-MM-DD HH:mm:ss"),
                "input_filename": st.column_config.TextColumn("Input File"),
                "total_rows": st.column_config.NumberColumn("Total Rows"),
                "processed_rows": st.column_config.ProgressColumn(
                    "Progress",
                    help="Number of rows processed",
                    format="%f",
                    min_value=0,
                    max_value=jobs_df['total_rows'].max(), # A bit of a hack, but works visually
                ),
                "output_filename": st.column_config.LinkColumn(
                    "Output File",
                    help="Download link for the processed file",
                    display_text="Download"
                ),
                "error_message": st.column_config.TextColumn("Error"),
            },
            use_container_width=True
        )
        # This is a placeholder for the download link. Streamlit can't serve files directly this way.
        # A proper implementation would require a small web server or a pre-signed URL from a cloud storage.
        # For now, the user would need to use the Filebrowser method to retrieve files.
    else:
        st.write("No jobs found.")
else:
    st.warning("Could not connect to the database to fetch jobs.")

# Auto-refresh the page every 30 seconds
time.sleep(30)
st.experimental_rerun()
