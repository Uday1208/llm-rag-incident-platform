# tools/export_docs_for_resolution.py
# Exports documents (id, ts, source, severity, content) to a CSV you can edit
import os, csv, psycopg2

MIN_SEV = os.getenv("EXPORT_MIN_SEVERITY", "ERROR").upper()   # ERROR or WARNING
LIMIT   = int(os.getenv("EXPORT_LIMIT", "500"))
OUT     = os.getenv("EXPORT_OUT", "resolution_seed.csv")

def main():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    sql = """
    SELECT id, ts, source, COALESCE(severity, 'INFO') AS severity, content
    FROM documents
    WHERE severity IN ('ERROR','CRITICAL')  -- tighten first pass
    ORDER BY ts DESC
    LIMIT %s
    """
    with conn, conn.cursor() as cur:
        cur.execute(sql, (LIMIT,))
        rows = cur.fetchall()

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # You will fill the last 5 columns
        w.writerow([
            "doc_id","ts","source","severity","content",
            "res_summary","res_actions_pipe","verification","outcome","confidence"
        ])
        for r in rows:
            doc_id, ts, source, sev, content = r
            w.writerow([doc_id, ts, source, sev, content, "", "", "", "", ""])

    print(f"[export] wrote {len(rows)} rows to {OUT}")

if __name__ == "__main__":
    main()
