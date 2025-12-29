import os
import psycopg2
from dotenv import load_dotenv

# 自动加载 f:\Dev\AIstock\.env 中的数据库配置
load_dotenv()


def get_db_cfg():
    return {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
    }


def main():
    cfg = get_db_cfg()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    cur = conn.cursor()

    print("=== 当前连接情况 (pg_stat_activity) ===")
    cur.execute(
        """
        SELECT pid, usename, application_name, client_addr,
               state, wait_event_type, wait_event,
               query_start, now() - query_start AS dur,
               LEFT(query, 120)
        FROM pg_stat_activity
        WHERE datname = %s
        ORDER BY query_start DESC
        LIMIT 50;
        """,
        (cfg["dbname"],),
    )
    rows = cur.fetchall()
    for r in rows:
        print(
            f"pid={r[0]}, user={r[1]}, app={r[2]}, state={r[4]}, "
            f"wait={r[5]}/{r[6]}, dur={r[8]}, sql={r[9]}"
        )

    print("\n=== 统计 AIstock-backend 使用的连接数 ===")
    cur.execute(
        """
        SELECT state, count(*)
        FROM pg_stat_activity
        WHERE datname = %s AND application_name = 'AIstock-backend'
        GROUP BY state;
        """,
        (cfg["dbname"],),
    )
    for state, cnt in cur.fetchall():
        print(f"state={state}, count={cnt}")

    print("\n=== 最近慢查询 (需要开启 pg_stat_statements) ===")
    try:
        cur.execute(
            """
            SELECT calls, total_time, mean_time, rows,
                   LEFT(query, 120)
            FROM pg_stat_statements
            ORDER BY mean_time DESC
            LIMIT 20;
            """
        )
        for r in cur.fetchall():
            print(
                f"calls={r[0]}, mean_time={r[2]:.2f} ms, rows={r[3]}, sql={r[4]}"
            )
    except Exception as e:
        print(f"无法读取 pg_stat_statements: {e}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
