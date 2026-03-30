"""从数据库查询因子的历史表现"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import psycopg2

def main():
    factor_name = "Earnings_Growth_Acceleration"

    print(f"=== {factor_name} 数据库表现查询 ===\n")

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="aistock",
        user="aistock_user",
        password="aistock_pass"
    )
    cur = conn.cursor()

    # 查询因子目录
    cur.execute("""
        SELECT factor_name, category, ic, icir, sharpe_ratio,
               annual_return, max_drawdown, created_at
        FROM aistock_factor_catalog
        WHERE factor_name = %s
    """, (factor_name,))

    result = cur.fetchone()

    if result:
        print("因子表现指标:")
        print(f"  类别: {result[1]}")
        print(f"  IC: {result[2]:.6f}" if result[2] else "  IC: N/A")
        print(f"  ICIR: {result[3]:.4f}" if result[3] else "  ICIR: N/A")
        print(f"  Sharpe: {result[4]:.4f}" if result[4] else "  Sharpe: N/A")
        print(f"  年化收益: {result[5]:.2%}" if result[5] else "  年化收益: N/A")
        print(f"  最大回撤: {result[6]:.2%}" if result[6] else "  最大回撤: N/A")
        print(f"  创建时间: {result[7]}")
    else:
        print(f"[INFO] 因子 '{factor_name}' 不在数据库中")
        print("\n查询所有 Earnings 相关因子...")

        cur.execute("""
            SELECT factor_name, ic, icir, annual_return
            FROM aistock_factor_catalog
            WHERE factor_name ILIKE %s
            ORDER BY ic DESC NULLS LAST
            LIMIT 10
        """, ('%Earnings%',))

        similar = cur.fetchall()
        if similar:
            print(f"\n找到 {len(similar)} 个相关因子:")
            for row in similar:
                ic_str = f"{row[1]:.4f}" if row[1] else "N/A"
                icir_str = f"{row[2]:.4f}" if row[2] else "N/A"
                ret_str = f"{row[3]:.2%}" if row[3] else "N/A"
                print(f"  {row[0]:<40} IC={ic_str:>8} ICIR={icir_str:>8} Ret={ret_str:>8}")
        else:
            print("  未找到相关因子")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
