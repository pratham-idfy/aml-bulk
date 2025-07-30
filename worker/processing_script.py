import os
import csv
import requests
import psycopg2
import time
import uuid
from datetime import datetime
from requests.exceptions import Timeout

# --- Configuration ---
# These will be pulled from environment variables on Railway
API_URL = 'https://api.idfy.com/v3/tasks/sync/verify_with_source/aml'
API_KEY = os.getenv('API_KEY', 'd06a7783-fd68-4a6b-9b04-294c8d8be9f9') 
ACCOUNT_ID = os.getenv('ACCOUNT_ID', 'f0e1a9d8f0d6/62550fb3-75a8-480a-a48c-35ea76440acf')
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/bulk_jobs_db")

TIMEOUT_SECONDS = 120
MAX_RETRIES = 3
RETRY_DELAY = 1

# --- Database Functions ---
def get_db_connection():
    """Creates a connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL)

def update_job_status(job_id, status, error_message=None):
    """Updates the status of a job in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE jobs SET status = %s, error_message = %s, end_timestamp = %s WHERE job_id = %s",
        (status, error_message, datetime.utcnow(), job_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

def update_job_progress(job_id, processed_rows):
    """Updates the number of processed rows for a job."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE jobs SET processed_rows = %s WHERE job_id = %s",
        (processed_rows, job_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

# --- API Call Functions (remains the same as before) ---
def prepare_api_payload(full_name, entity_type, dob=None, pan_number=None):
    filters = {
        "types": ["sanctions", "pep", "warnings"],
        "name_fuzziness": "1",
        "search_profile": "all_default",
        "country_codes": ["IN"],
        "entity_type": entity_type.lower()
    }
    if entity_type.lower() == 'individual' and dob and dob.strip():
        filters["birth_year"] = dob.strip()
    if pan_number and pan_number.strip():
        filters["pan_number"] = pan_number.strip()
    return {
        "task_id": str(uuid.uuid4()),
        "group_id": str(uuid.uuid4()),
        "data": {
            "search_term": full_name,
            "filters": filters,
            "version": "2",
            "get_profile_pdf": False
        }
    }

def make_api_call(full_name, entity_type, dob=None, pan_number=None):
    headers = {
        'Content-Type': 'application/json',
        'api-key': API_KEY,
        'account-id': ACCOUNT_ID
    }
    data = prepare_api_payload(full_name, entity_type, dob, pan_number)
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(API_URL, headers=headers, json=data, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json(), response.status_code, None
        except (Timeout, requests.exceptions.RequestException) as e:
            error_message = f"Error on attempt {attempt + 1}: {str(e)}"
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                return None, getattr(e.response, 'status_code', -1), error_message
    return None, -1, "Max retries reached"

# --- Main Processing Function ---
def run_bulk_job(job_id, input_file_path, output_file_path):
    """The main function to be called by the RQ worker."""
    print(f"Starting job {job_id} for file {input_file_path}")
    update_job_status(job_id, "running")

    try:
        with open(input_file_path, mode='r', newline='', errors='ignore') as infile, \
             open(output_file_path, mode='w', newline='') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            header = next(reader)
            writer.writerow(header + ["api_status_code", "match_status", "total_hits", "api_error"])

            processed_count = 0
            for row in reader:
                if len(row) < 3:
                    continue

                uid, full_name, entity_type = row[0], row[1].strip(), row[2].strip()
                dob = row[3].strip() if len(row) > 3 else None
                pan_number = row[4].strip() if len(row) > 4 else None

                response_json, status_code, error = make_api_call(full_name, entity_type, dob, pan_number)

                match_status = None
                total_hits = None
                if response_json and response_json.get("status") == "success":
                    result = response_json.get("result", {})
                    match_status = result.get("match_status")
                    total_hits = result.get("total_hits")
                
                writer.writerow(row + [status_code, match_status, total_hits, error])

                processed_count += 1
                if processed_count % 10 == 0:
                    update_job_progress(job_id, processed_count)
                    print(f"Job {job_id}: Processed {processed_count} rows.")
            
            update_job_progress(job_id, processed_count)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE jobs SET output_file_path = %s WHERE job_id = %s", (output_file_path, job_id))
        conn.commit()
        cursor.close()
        conn.close()
        update_job_status(job_id, "completed")
        print(f"Job {job_id} completed successfully. Output at {output_file_path}")

    except Exception as e:
        print(f"Job {job_id} failed: {str(e)}")
        update_job_status(job_id, "failed", error_message=str(e))