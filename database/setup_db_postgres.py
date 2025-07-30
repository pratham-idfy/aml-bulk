
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# This script is for reference and local setup. 
# On Railway, the database is provisioned automatically.

# Get the database URL from environment variables
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/bulk_jobs_db")

def setup_database():
    """Initializes the database and creates the jobs table if it doesn't exist."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError:
        # If the database does not exist, create it
        # This part is mainly for local setup
        conn_info = psycopg2.connect(DATABASE_URL, dbname="postgres")
        conn_info.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn_info.cursor()
        cursor.execute("CREATE DATABASE bulk_jobs_db")
        cursor.close()
        conn_info.close()
        # Retry connection
        conn = psycopg2.connect(DATABASE_URL)

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
    print("Database and jobs table are set up for PostgreSQL.")

if __name__ == "__main__":
    # This allows running the script manually if needed for local setup
    setup_database()
