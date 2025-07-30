
import sqlite3

def setup_database():
    """Initializes the database and creates the jobs table if it doesn't exist."""
    conn = sqlite3.connect("/Users/prathamshetty/Documents/AML V2/IPO/bulk_job_app/database/jobs.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            start_timestamp DATETIME NOT NULL,
            end_timestamp DATETIME,
            input_file_path TEXT NOT NULL,
            output_file_path TEXT,
            total_rows INTEGER,
            processed_rows INTEGER DEFAULT 0,
            error_message TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("Database and jobs table are set up.")

if __name__ == "__main__":
    setup_database()
