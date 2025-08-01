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
st.set_page_config(page_title="Bulk Job Runner", layout="wide")

# --- Environment Variables & Connections ---
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AUTH_CODE = os.getenv("AUTH_CODE")
MAX_FILE_SIZE_MB = 20
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

# --- Persistent Storage Setup ---
UPLOAD_FOLDER = "/data"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Database Functions ---
def get_db_connection():
    if not DATABASE_URL: st.error("Database URL is not configured."); return None
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    if "reconciliation_done" in st.session_state: return
    conn = get_db_connection()
    if not conn: return
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY, job_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
                start_timestamp TIMESTAMPTZ NOT NULL, end_timestamp TIMESTAMPTZ,
                input_filename TEXT NOT NULL, output_filename TEXT,
                total_rows INTEGER, processed_rows INTEGER DEFAULT 0, error_message TEXT
            );
        """)
        cursor.execute("UPDATE jobs SET status = 'failed', error_message = 'App restarted' WHERE status = 'running';")
    conn.commit()
    conn.close()
    st.session_state.reconciliation_done = True
    print("Database setup and reconciliation complete.")

# --- API & Processing Logic ---
def update_job_status(job_id, status, error_message=None):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("UPDATE jobs SET status = %s, error_message = %s, end_timestamp = %s WHERE job_id = %s", (status, error_message, datetime.utcnow(), job_id))
    conn.commit()
    conn.close()

def update_job_progress(job_id, processed_rows):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("UPDATE jobs SET processed_rows = %s WHERE job_id = %s", (processed_rows, job_id))
    conn.commit()
    conn.close()

def fetch_hits_json(hits_url):
    if not hits_url: return None
    try:
        response = requests.get(hits_url, timeout=60)
        response.raise_for_status()
        return response.text
    except Exception as e:
        return f"Error fetching hits: {str(e)}"

def make_api_call(full_name, entity_type, dob=None, pan_number=None):
    headers = {'Content-Type': 'application/json', 'api-key': API_KEY, 'account-id': ACCOUNT_ID}
    filters = {"types": ["sanctions"], "name_fuzziness": "1", "search_profile": "all_default", "country_codes": ["IN"], "entity_type": entity_type.lower()}
    if entity_type.lower() == 'individual' and dob and dob.strip(): filters["birth_year"] = dob.strip()
    if pan_number and pan_number.strip(): filters["pan_number"] = pan_number.strip()
    data = {"task_id": str(uuid.uuid4()), "group_id": str(uuid.uuid4()), "data": {"search_term": full_name, "filters": filters, "version": "2", "get_profile_pdf": False}}
    for _ in range(3):
        try:
            response = requests.post('https://api.idfy.com/v3/tasks/sync/verify_with_source/aml', headers=headers, json=data, timeout=120)
            response.raise_for_status()
            return response.json(), response.status_code, None
        except (Timeout, requests.exceptions.RequestException) as e:
            time.sleep(1)
    return None, -1, "Max retries reached"

def run_bulk_job(job_id, input_filepath, output_filepath):
    try:
        update_job_status(job_id, "running")
        with open(input_filepath, 'r', newline='', errors='ignore') as infile, open(output_filepath, 'w', newline='') as outfile:
            reader, writer = csv.reader(infile), csv.writer(outfile)
            try:
                header = next(reader)
                writer.writerow(header + ["api_status_code", "match_status", "total_hits", "api_error", "hits_json"])
            except StopIteration:
                raise ValueError("Input file is empty or invalid.")
            processed_count = 0
            for row in reader:
                if len(row) < 3: continue
                _, full_name, entity_type = row[0], row[1].strip(), row[2].strip()
                dob, pan_number = (row[3].strip() if len(row) > 3 else None), (row[4].strip() if len(row) > 4 else None)
                response_json, status_code, error = make_api_call(full_name, entity_type, dob, pan_number)
                match_status, total_hits, hits_json = None, None, None
                if status_code == 200 and response_json:
                    result = response_json.get("result", {})
                    match_status, total_hits, hits_url = result.get("match_status"), result.get("total_hits"), result.get("hits")
                    hits_json = fetch_hits_json(hits_url)
                writer.writerow(row + [status_code, match_status, total_hits, error, hits_json])
                time.sleep(1)
                processed_count += 1
                if processed_count % 5 == 0: update_job_progress(job_id, processed_count)
            update_job_progress(job_id, processed_count)
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE jobs SET output_filename = %s WHERE job_id = %s", (os.path.basename(output_filepath), job_id))
        conn.commit()
        conn.close()
        update_job_status(job_id, "completed")
    except Exception as e:
        update_job_status(job_id, "failed", error_message=str(e))

# --- Main Application Logic ---
def main_app():
    st.title("Bulk AML Check Runner")
    setup_database()

    st.header("Submit a New Job")
    st.markdown(f"**Upload a CSV file (Maximum size: {MAX_FILE_SIZE_MB}MB)**")
    uploaded_file = st.file_uploader("Upload CSV", type="csv", label_visibility="collapsed")
    if uploaded_file:
        if uploaded_file.size > MAX_FILE_SIZE_BYTES:
            st.error(f"File is too large ({uploaded_file.size / 1024 / 1024:.1f}MB). Maximum size is {MAX_FILE_SIZE_MB}MB.")
        elif st.button("Start Processing"):
            job_id = str(uuid.uuid4())
            original_filename = uploaded_file.name
            input_filepath = os.path.join(UPLOAD_FOLDER, f"{job_id}_{original_filename}")
            with open(input_filepath, "wb") as f: f.write(uploaded_file.getbuffer())
            with open(input_filepath, 'r', errors='ignore') as f: total_rows = sum(1 for _ in f) - 1
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO jobs (job_id, status, start_timestamp, input_filename, total_rows) VALUES (%s, %s, %s, %s, %s)", (job_id, "pending", datetime.utcnow(), original_filename, total_rows))
            conn.commit()
            conn.close()
            output_filepath = os.path.join(UPLOAD_FOLDER, f"{job_id}_output.csv")
            thread = threading.Thread(target=run_bulk_job, args=(job_id, input_filepath, output_filepath))
            thread.start()
            st.success(f"Job submitted! Refreshing...")
            time.sleep(2)
            st.rerun()

    st.header("Job Dashboard")
    conn = get_db_connection()
    jobs_df = pd.DataFrame()
    if conn:
        try:
            jobs_df = pd.read_sql("SELECT * FROM jobs ORDER BY start_timestamp DESC", conn)
        finally:
            conn.close()
    if not jobs_df.empty:
        jobs_df['Progress'] = jobs_df.apply(lambda r: f"{r['processed_rows']} / {r['total_rows']}", axis=1)
        st.dataframe(jobs_df, column_order=["job_id", "status", "start_timestamp", "input_filename", "Progress", "error_message"], column_config={
            "job_id": "Job ID", "status": "Status", "start_timestamp": "Submitted At",
            "input_filename": "Input File", "Progress": "Progress", "error_message": "Error"
        }, use_container_width=True, hide_index=True)
    else:
        st.info("No jobs found.")

    st.header("Download Results")
    if not jobs_df.empty:
        completed_jobs_df = jobs_df[jobs_df['status'] == 'completed']
        if not completed_jobs_df.empty:
            job_to_download = st.selectbox('Select a completed job', options=completed_jobs_df['job_id'], format_func=lambda x: f"{x[:8]}... ({completed_jobs_df.loc[completed_jobs_df.job_id == x, 'input_filename'].iloc[0]})" )
            if job_to_download:
                output_filename = completed_jobs_df.loc[completed_jobs_df.job_id == job_to_download, 'output_filename'].iloc[0]
                output_filepath = os.path.join(UPLOAD_FOLDER, output_filename)
                if os.path.exists(output_filepath):
                    with open(output_filepath, "rb") as f:
                        st.download_button("Download CSV", f, file_name=output_filename, mime='text/csv')
                else:
                    st.error("Output file not found in storage.")
        else:
            st.info("No completed jobs available for download.")

    time.sleep(30)
    st.rerun()

def login_screen():
    st.title("Application Access")
    st.header("Please enter the Auth Code to continue")
    auth_code_input = st.text_input("Auth Code", type="password")
    if st.button("Login"):
        if AUTH_CODE and auth_code_input == AUTH_CODE:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("The authentication code is incorrect.")

# --- Authentication Gate ---
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

if st.session_state.authenticated:
    main_app()
else:
    login_screen()