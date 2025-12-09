import os
import sys

from dotenv import load_dotenv
import psycopg2


def get_db_cfg():
    # 优先从 .env 中加载环境变量（如果存在）
    # 这样可以避免在命令行手工导出敏感信息
    load_dotenv()
    return dict(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )


def main() -> None:
    cfg = get_db_cfg()
    print("[INFO] Connecting to DB with config:", {k: (v if k != "password" else "***") for k, v in cfg.items()})
    try:
        with psycopg2.connect(**cfg) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'market' AND table_name = 'adj_factor'
                    """
                )
                exists_row = cur.fetchone()
                if not exists_row:
                    print("[RESULT] Table market.adj_factor DOES NOT exist.")
                    return

                print("[RESULT] Table market.adj_factor EXISTS.")
                print("[INFO] Columns:")
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'market' AND table_name = 'adj_factor'
                    ORDER BY ordinal_position
                    """
                )
                for name, dtype, nullable in cur.fetchall():
                    print(f"  - {name}: {dtype}, nullable={nullable}")

                print("[INFO] Primary key:")
                cur.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = 'market'
                      AND tc.table_name = 'adj_factor'
                      AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                    """
                )
                pk_cols = [r[0] for r in cur.fetchall()]
                if pk_cols:
                    print("  - PK:(" + ", ".join(pk_cols) + ")")
                else:
                    print("  - (no primary key defined)")

    except Exception as e:  # noqa: BLE001
        print("[ERROR] Failed to check market.adj_factor:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
