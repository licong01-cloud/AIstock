#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试 QMT/xtquant 连接和持仓查询
"""
import sys
from pathlib import Path
from dotenv import load_dotenv

# 加载 .env 文件
project_root = Path(__file__).resolve().parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"已加载 .env 文件: {env_file}")
else:
    print(f"警告: 未找到 .env 文件: {env_file}")

# 确保能找到 backend 模块
sys.path.insert(0, str(project_root))

from backend.infra.qmt_client import build_qmt_client_from_env

def main():
    print("=" * 60)
    print("测试 QMT/xtquant 连接")
    print("=" * 60)
    
    # 构建客户端
    print("\n1. 构建 QMT 客户端...")
    try:
        client = build_qmt_client_from_env()
        status = client.status()
        print(f"   状态: {status}")
    except Exception as e:
        print(f"   ❌ 构建失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 检查是否可用
    if status.provider == "simulator":
        print(f"\n   [WARN] xtquant 不可用: {status.last_error or '未知原因'}")
        print("   提示: 请检查:")
        print("      - xtquant 目录是否存在: F:\\Dev\\AIstock\\xtquant")
        print("      - Python 版本是否匹配 (需要 64位)")
        print("      - .env 中 MINIQMT_USERDATA_PATH 是否配置正确")
        return
    
    if not status.enabled:
        print(f"\n   [WARN] QMT 未启用 (MINIQMT_ENABLED=false)")
        print("   提示: 请在 .env 中设置 MINIQMT_ENABLED=true")
        return
    
    print(f"   [OK] xtquant 可用 (provider: {status.provider})")
    
    # 连接
    print("\n2. 连接 QMT...")
    try:
        ok, msg = client.connect()
        if ok:
            print(f"   [OK] 连接成功: {msg}")
        else:
            print(f"   [FAIL] 连接失败: {msg}")
            print("   提示: 请确保:")
            print("      - miniQMT 客户端已启动并登录")
            print("      - .env 中 MINIQMT_USERDATA_PATH 指向正确的 userdata_mini 目录")
            print("      - .env 中 MINIQMT_SESSION_ID 已配置")
            return
    except Exception as e:
        print(f"   ❌ 连接异常: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 查询资金
    print("\n3. 查询资金信息...")
    try:
        account = client.get_account_info()
        print(f"   [OK] 资金信息:")
        for k, v in account.items():
            print(f"      {k}: {v}")
    except Exception as e:
        print(f"   [FAIL] 查询失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 查询持仓
    print("\n4. 查询持仓信息...")
    try:
        positions = client.get_positions()
        if positions:
            print(f"   [OK] 持仓数量: {len(positions)}")
            for i, pos in enumerate(positions[:5], 1):  # 只显示前5个
                print(f"      持仓 {i}: {pos}")
            if len(positions) > 5:
                print(f"      ... 还有 {len(positions) - 5} 个持仓")
        else:
            print("   [INFO] 当前无持仓")
    except Exception as e:
        print(f"   [FAIL] 查询失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 断开连接
    print("\n5. 断开连接...")
    try:
        ok, msg = client.disconnect()
        if ok:
            print(f"   [OK] 断开成功")
        else:
            print(f"   [WARN] 断开失败: {msg}")
    except Exception as e:
        print(f"   [WARN] 断开异常: {e}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main()

