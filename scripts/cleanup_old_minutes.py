"""删除 market.kline_minute_raw 中 2024 年之前的历史数据
使用 TimescaleDB drop_chunks 高效删除整个 chunk（不需要 VACUUM）"""
import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
cur = conn.cursor()

# 1. 删除前统计
cur.execute("SELECT COUNT(*) FROM market.kline_minute_raw WHERE trade_time < '2024-01-01'")
old_count = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM market.kline_minute_raw")
total_count = cur.fetchone()[0]
print(f'删除前: 总行数 {total_count:,}, 2024前 {old_count:,} ({old_count/total_count*100:.1f}%)')

# 2. 查看将被删除的 chunks 数量
cur.execute("""
    SELECT COUNT(*) FROM timescaledb_information.chunks
    WHERE hypertable_schema = 'market' AND hypertable_name = 'kline_minute_raw'
    AND range_end <= '2024-01-01'::timestamptz
""")
chunks_to_drop = cur.fetchone()[0]
print(f'将删除 {chunks_to_drop} 个 chunks')

# 3. 删除前磁盘占用
cur.execute("""
    SELECT pg_size_pretty(pg_total_relation_size('market.kline_minute_raw'))
""")
size_before = cur.fetchone()[0]
print(f'删除前磁盘占用: {size_before}')

# 4. 执行 drop_chunks
print('\n执行 drop_chunks (older_than 2024-01-01)...')
cur.execute("""
    SELECT drop_chunks('market.kline_minute_raw', older_than => '2024-01-01'::timestamptz)
""")
dropped = cur.fetchall()
print(f'已删除 {len(dropped)} 个 chunks')

conn.commit()

# 5. 删除后统计
cur.execute("SELECT COUNT(*) FROM market.kline_minute_raw")
new_total = cur.fetchone()[0]
cur.execute("""
    SELECT pg_size_pretty(pg_total_relation_size('market.kline_minute_raw'))
""")
size_after = cur.fetchone()[0]

cur.execute("""
    SELECT MIN(trade_time), MAX(trade_time) FROM market.kline_minute_raw
""")
min_t, max_t = cur.fetchone()

print(f'\n=== 删除完成 ===')
print(f'  删除前: {total_count:,} 行, {size_before}')
print(f'  删除后: {new_total:,} 行, {size_after}')
print(f'  删除行数: {total_count - new_total:,}')
print(f'  剩余时间范围: {min_t} ~ {max_t}')

conn.close()
