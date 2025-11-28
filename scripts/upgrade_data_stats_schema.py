import os
import psycopg2

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)

def main():
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True
    cur = conn.cursor()
    
    print("Checking market.data_stats schema...")
    
    # Check columns
    cur.execute("SELECT * FROM market.data_stats LIMIT 0")
    cols = [desc[0] for desc in cur.description]
    
    if "last_check_result" not in cols:
        print("Adding column last_check_result...")
        cur.execute("ALTER TABLE market.data_stats ADD COLUMN last_check_result JSONB DEFAULT NULL")
    
    if "last_check_at" not in cols:
        print("Adding column last_check_at...")
        cur.execute("ALTER TABLE market.data_stats ADD COLUMN last_check_at TIMESTAMPTZ DEFAULT NULL")
        
    print("Schema upgrade complete.")
    conn.close()

if __name__ == "__main__":
    main()
