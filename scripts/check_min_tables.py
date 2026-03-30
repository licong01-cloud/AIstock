"""检查 market.kline_minute_raw 表的数据分布"""
import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
cur = conn.cursor()

# 1. 表大小
cur.execute("""
    SELECT pg_size_pretty(pg_total_relation_size('market.kline_minute_raw')) as total,
           pg_size_pretty(pg_relation_size('market.kline_minute_raw')) as data,
           pg_size_pretty(pg_indexes_size('market.kline_minute_raw')) as idx
""")
total, data, idx = cur.fetchone()
print(f'=== market.kline_minute_raw ===')
print(f'  总大小: {total} (数据: {data}, 索引: {idx})')

# 2. 总行数
cur.execute("SELECT COUNT(*) FROM market.kline_minute_raw")
total_rows = cur.fetchone()[0]
print(f'  总行数: {total_rows:,}')

# 3. 列结构
cur.execute("""
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_schema = 'market' AND table_name = 'kline_minute_raw'
    ORDER BY ordinal_position
""")
print(f'\n  列结构:')
for col, dtype in cur.fetchall():
    print(f'    {col}: {dtype}')

# 4. 按年统计
cur.execute("""
    SELECT EXTRACT(YEAR FROM trade_time)::int as yr, COUNT(*) as cnt
    FROM market.kline_minute_raw
    GROUP BY yr ORDER BY yr
""")
print(f'\n  按年分布:')
rows_before_2024 = 0
for yr, cnt in cur.fetchall():
    marker = ' ← 可删除' if yr < 2024 else ' ← 保留'
    print(f'    {yr}: {cnt:>15,}{marker}')
    if yr < 2024:
        rows_before_2024 += cnt

pct = rows_before_2024 / total_rows * 100 if total_rows > 0 else 0
print(f'\n  2024前数据: {rows_before_2024:,} / {total_rows:,} ({pct:.1f}%)')

# 5. 估算删除后节省空间
avg_row_size = cur.execute("SELECT pg_relation_size('market.kline_minute_raw')") or 0
cur.execute("SELECT pg_relation_size('market.kline_minute_raw')")
data_bytes = cur.fetchone()[0]
if total_rows > 0:
    bytes_per_row = data_bytes / total_rows
    saved_bytes = rows_before_2024 * bytes_per_row
    print(f'  估算节省: {saved_bytes/1024/1024/1024:.1f} GB')

# 6. 检查是否为 hypertable
cur.execute("""
    SELECT EXISTS(
        SELECT 1 FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'market' AND hypertable_name = 'kline_minute_raw'
    )
""")
is_hyper = cur.fetchone()[0]
print(f'\n  TimescaleDB hypertable: {is_hyper}')

if is_hyper:
    # 查看 chunk 分布
    cur.execute("""
        SELECT chunk_schema, chunk_name,
               pg_size_pretty(pg_total_relation_size(chunk_schema||'.'||chunk_name)) as size,
               range_start, range_end
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = 'market' AND hypertable_name = 'kline_minute_raw'
        ORDER BY range_start
        LIMIT 30
    """)
    chunks = cur.fetchall()
    print(f'  Chunks 数量: {len(chunks)}+')
    print(f'  前几个 chunks:')
    for schema, name, size, rs, re in chunks[:10]:
        marker = ' ← DROP' if re and str(re) < '2024-01-01' else ''
        print(f'    {rs} ~ {re}: {size}{marker}')

conn.close()
