"""策略执行诊断工具

检查策略为什么没有产生交易。
"""
import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from backend.db.pg_pool import get_conn
from backend.infra.qmt_client import build_qmt_client_from_env
from backend.schedulers.strategy_scheduler import scheduler
from backend.strategies.ma_cross_strategy import MACrossStrategy
from backend.strategies.trend_following_strategy import TrendFollowingStrategy
from backend.infra.strategy_executor import SimpleStrategyExecutor
import json
from datetime import datetime

def check_qmt_connection():
    """检查QMT连接状态"""
    print("=" * 60)
    print("1. 检查QMT连接状态")
    print("=" * 60)
    client = build_qmt_client_from_env()
    status = client.status()
    print(f"QMT启用: {status.enabled}")
    print(f"QMT连接: {status.connected}")
    print(f"模式: {status.mode}")
    print(f"账户ID: {status.account_id}")
    print(f"提供者: {status.provider}")
    print(f"最后错误: {status.last_error}")
    
    if not status.connected:
        print("\n⚠️ 警告: QMT未连接，策略无法下单！")
        print("请先调用 /api/v1/qmt/connect 连接QMT")
        return False
    return True

def check_strategy_configs():
    """检查策略配置"""
    print("\n" + "=" * 60)
    print("2. 检查策略配置")
    print("=" * 60)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT strategy_id, strategy_name, strategy_type, enabled,
                       config_json, schedule_config
                FROM trading.strategy_config
                WHERE enabled = TRUE
                ORDER BY strategy_id
            """)
            rows = cur.fetchall()
    
    if not rows:
        print("❌ 没有启用的策略")
        return []
    
    strategies = []
    for row in rows:
        strategy_id, strategy_name, strategy_type, enabled, config_json, schedule_config = row
        
        # 解析JSON
        if isinstance(config_json, dict):
            config = config_json
        elif isinstance(config_json, str):
            config = json.loads(config_json) if config_json else {}
        else:
            config = {}
        
        if isinstance(schedule_config, dict):
            schedule = schedule_config
        elif isinstance(schedule_config, str):
            schedule = json.loads(schedule_config) if schedule_config else {}
        else:
            schedule = {}
        
        strategies.append({
            "id": strategy_id,
            "name": strategy_name,
            "type": strategy_type,
            "config": config,
            "schedule": schedule,
        })
        
        print(f"\n策略ID: {strategy_id}")
        print(f"策略名称: {strategy_name}")
        print(f"策略类型: {strategy_type}")
        print(f"调度类型: {schedule.get('type', 'daily')}")
        print(f"调度时间/间隔: {schedule.get('time', 'N/A')} / {schedule.get('interval', 'N/A')}")
        print(f"股票列表: {config.get('symbols', [])}")
        print(f"周期: {config.get('period', '1d')}")
        
        # 检查5分钟策略配置
        if schedule.get('type') == 'minute':
            interval = schedule.get('interval', 1)
            if interval != 5:
                print(f"⚠️ 警告: 5分钟策略的interval配置为{interval}分钟，应该是5")
        elif schedule.get('type') == 'realtime':
            print("ℹ️ 使用实时行情触发")
        else:
            print(f"⚠️ 警告: 调度类型为{schedule.get('type')}，不是5分钟策略")
    
    return strategies

def check_strategy_executions():
    """检查策略执行记录"""
    print("\n" + "=" * 60)
    print("3. 检查策略执行记录（最近10条）")
    print("=" * 60)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, strategy_id, execution_type, trigger_source, status,
                       start_time, end_time, signals_generated, signals_executed,
                       symbols_processed, error_message
                FROM trading.strategy_execution
                ORDER BY start_time DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
    
    if not rows:
        print("❌ 没有执行记录")
        return
    
    for row in rows:
        exec_id, strategy_id, exec_type, trigger, status, start_time, end_time, \
        signals_gen, signals_exec, symbols_proc, error = row
        
        print(f"\n执行ID: {exec_id}")
        print(f"策略ID: {strategy_id}")
        print(f"执行类型: {exec_type}")
        print(f"触发源: {trigger}")
        print(f"状态: {status}")
        print(f"开始时间: {start_time}")
        print(f"结束时间: {end_time}")
        print(f"生成信号数: {signals_gen}")
        print(f"执行信号数: {signals_exec}")
        print(f"处理股票数: {symbols_proc}")
        if error:
            print(f"❌ 错误: {error}")

def check_trade_intents():
    """检查交易意图记录"""
    print("\n" + "=" * 60)
    print("4. 检查交易意图记录（最近10条）")
    print("=" * 60)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, strategy_id, symbol, side, quantity, price_type, price,
                       status, reason, created_at, executed_at, order_id, error_message
                FROM trading.trade_intent
                ORDER BY created_at DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
    
    if not rows:
        print("❌ 没有交易意图记录")
        return
    
    for row in rows:
        intent_id, strategy_id, symbol, side, qty, price_type, price, \
        status, reason, created_at, executed_at, order_id, error = row
        
        print(f"\n意图ID: {intent_id}")
        print(f"策略ID: {strategy_id}")
        print(f"股票: {symbol}")
        print(f"方向: {side}")
        print(f"数量: {qty}")
        print(f"价格类型: {price_type}")
        print(f"价格: {price}")
        print(f"状态: {status}")
        print(f"原因: {reason}")
        print(f"创建时间: {created_at}")
        if executed_at:
            print(f"执行时间: {executed_at}")
        if order_id:
            print(f"订单ID: {order_id}")
        if error:
            print(f"❌ 错误: {error}")

def test_strategy_execution(strategy_id: str, symbol: str):
    """测试策略执行"""
    print("\n" + "=" * 60)
    print(f"5. 测试策略执行: {strategy_id} on {symbol}")
    print("=" * 60)
    
    # 获取策略配置
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT strategy_type, config_json
                FROM trading.strategy_config
                WHERE strategy_id = %s
            """, (strategy_id,))
            row = cur.fetchone()
    
    if not row:
        print(f"❌ 策略 {strategy_id} 不存在")
        return
    
    strategy_type, config_json = row
    
    # 解析配置
    if isinstance(config_json, dict):
        config = config_json
    elif isinstance(config_json, str):
        config = json.loads(config_json) if config_json else {}
    else:
        config = {}
    
    # 创建策略实例
    executor = SimpleStrategyExecutor()
    if strategy_type == "MA_CROSS":
        strategy = MACrossStrategy(strategy_id=strategy_id, executor=executor, config=config)
    elif strategy_type == "TREND_FOLLOWING":
        strategy = TrendFollowingStrategy(strategy_id=strategy_id, executor=executor, config=config)
    else:
        print(f"❌ 未知策略类型: {strategy_type}")
        return
    
    # 执行策略
    print(f"执行策略 {strategy_id} 对股票 {symbol}...")
    result = strategy.run(symbol)
    
    print(f"\n执行结果:")
    print(f"成功: {result.get('success')}")
    print(f"消息: {result.get('message')}")
    print(f"信号数: {len(result.get('signals', []))}")
    if result.get('order_id'):
        print(f"订单ID: {result.get('order_id')}")
    
    if result.get('signals'):
        for sig in result.get('signals', []):
            print(f"\n信号详情:")
            print(f"  股票: {sig.get('symbol')}")
            print(f"  方向: {sig.get('side')}")
            print(f"  数量: {sig.get('quantity')}")
            print(f"  价格: {sig.get('price')}")
            print(f"  原因: {sig.get('reason')}")

def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("策略执行诊断工具")
    print("=" * 60)
    print(f"当前时间: {datetime.now()}")
    
    # 1. 检查QMT连接
    qmt_ok = check_qmt_connection()
    
    # 2. 检查策略配置
    strategies = check_strategy_configs()
    
    # 3. 检查执行记录
    check_strategy_executions()
    
    # 4. 检查交易意图
    check_trade_intents()
    
    # 5. 如果提供了参数，测试策略执行
    if len(sys.argv) >= 3:
        strategy_id = sys.argv[1]
        symbol = sys.argv[2]
        test_strategy_execution(strategy_id, symbol)
    elif strategies:
        # 测试第一个策略的第一个股票
        first_strategy = strategies[0]
        symbols = first_strategy['config'].get('symbols', [])
        if symbols:
            print("\n" + "=" * 60)
            print("提示: 可以运行以下命令测试策略执行:")
            print(f"python -m backend.scripts.diagnose_strategy {first_strategy['id']} {symbols[0]}")
    
    print("\n" + "=" * 60)
    print("诊断完成")
    print("=" * 60)

if __name__ == "__main__":
    main()

