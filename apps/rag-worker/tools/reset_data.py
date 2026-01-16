import os
import sys
import psycopg2

def reset_data():
    """Truncates the incidents, incident_resolutions, and documents tables."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set")
        sys.exit(1)
        
    print(f"Connecting to DB...")
    try:
        conn = psycopg2.connect(dsn)
        with conn, conn.cursor() as cur:
            print("Truncating tables...")
            cur.execute("TRUNCATE TABLE incidents CASCADE;")
            cur.execute("TRUNCATE TABLE incident_resolutions CASCADE;")
            cur.execute("TRUNCATE TABLE documents CASCADE;")
            conn.commit()
            print("Reset complete.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    reset_data()
