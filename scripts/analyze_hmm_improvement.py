#!/usr/bin/env python
"""HMM 模型优化的理论提升分析与回测验证方案.

分析维度:
1. 理论提升估算
2. 系数分布对比
3. 回测验证方案
4. 预期收益提升
"""
import sys
import json
import numpy as np
from collections import Counter

sys.stdout.reconfigure(encoding='utf-8')

def load_coefficients(coeff_path):
    """加载系数文件."""
    with open(coeff_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def analyze_coefficient_distribution(daily_coeffs):
    """分析系数分布."""
    all_coeffs = []
    for date, sectors in daily_coeffs.items():
        all_coeffs.extend(sectors.values())

    counter = Counter(all_coeffs)
    total = len(all_coeffs)

    return {
        'total_samples': total,
        'distribution': {
            coeff: {'count': count, 'pct': count/total*100}
            for coeff, count in sorted(counter.items())
        }
    }

def estimate_theoretical_improvement(old_dist, new_dist):
    """估算理论收益提升.

    假设:
    - trending 系数 1.05 → 实际超额收益 +5%
    - fading 系数 0.96 → 实际超额收益 -4%
    - neutral 系数 1.00 → 实际超额收益 0%

    改进来源:
    1. 状态识别准确率提升
    2. 状态持续时间延长 → 减少频繁切换损失
    """

    # 旧版本状态分布
    old_trending_pct = old_dist['distribution'].get(1.05, {}).get('pct', 0)
    old_neutral_pct = old_dist['distribution'].get(1.0, {}).get('pct', 0)
    old_fading_pct = old_dist['distribution'].get(0.96, {}).get('pct', 0)

    # 新版本状态分布
    new_trending_pct = new_dist['distribution'].get(1.05, {}).get('pct', 0)
    new_neutral_pct = new_dist['distribution'].get(1.0, {}).get('pct', 0)
    new_fading_pct = new_dist['distribution'].get(0.96, {}).get('pct', 0)

    print("\n" + "="*100)
    print("理论提升分析")
    print("="*100)

    print("\n1. 状态分布对比:")
    print(f"{'状态':12s} | {'旧版本':12s} | {'新版本':12s} | {'变化':12s}")
    print("-" * 55)
    print(f"{'Trending':12s} | {old_trending_pct:10.2f}% | {new_trending_pct:10.2f}% | {new_trending_pct-old_trending_pct:+10.2f}%")
    print(f"{'Neutral':12s} | {old_neutral_pct:10.2f}% | {new_neutral_pct:10.2f}% | {new_neutral_pct-old_neutral_pct:+10.2f}%")
    print(f"{'Fading':12s} | {old_fading_pct:10.2f}% | {new_fading_pct:10.2f}% | {new_fading_pct-old_fading_pct:+10.2f}%")

    # 估算收益影响
    print("\n2. 收益影响估算:")
    print("\n假设前提:")
    print("  - Trending 状态: 系数 1.05 → 年化超额收益 +5%")
    print("  - Neutral 状态:  系数 1.00 → 年化超额收益 0%")
    print("  - Fading 状态:   系数 0.96 → 年化超额收益 -4%")
    print("  - 状态识别准确率: 70% (行业研究经验值)")

    # 旧版本期望收益
    old_expected = (
        old_trending_pct/100 * 0.05 * 0.7 +  # trending 贡献
        old_fading_pct/100 * (-0.04) * 0.7    # fading 贡献
    )

    # 新版本期望收益
    new_expected = (
        new_trending_pct/100 * 0.05 * 0.7 +
        new_fading_pct/100 * (-0.04) * 0.7
    )

    print(f"\n旧版本期望超额收益: {old_expected*100:.2f}%")
    print(f"新版本期望超额收益: {new_expected*100:.2f}%")
    print(f"理论提升: {(new_expected-old_expected)*100:+.2f}%")

    # 状态稳定性改进
    print("\n3. 状态稳定性改进:")
    print("\n旧版本:")
    print("  - Trending 自转移: 23.5% → 期望持续 1.3 天")
    print("  - 频繁切换导致交易成本: ~0.3% 年化损失")

    print("\n新版本:")
    print("  - Trending 自转移: 41.9% → 期望持续 1.7 天")
    print("  - 切换频率降低 ~30% → 交易成本降低 ~0.1%")

    stability_improvement = 0.001  # 0.1% 年化

    # 总体提升
    total_improvement = (new_expected - old_expected) + stability_improvement

    print("\n" + "="*100)
    print("理论提升总结")
    print("="*100)
    print(f"\n1. 状态分布优化:    {(new_expected-old_expected)*100:+.2f}%")
    print(f"2. 稳定性改进:      {stability_improvement*100:+.2f}%")
    print(f"3. 总体理论提升:    {total_improvement*100:+.2f}%")

    print("\n预期效果:")
    if total_improvement > 0.005:
        print(f"  ✅ 显著提升 (>{total_improvement*100:.1f}%)")
    elif total_improvement > 0.002:
        print(f"  ✅ 中等提升 ({total_improvement*100:.1f}%)")
    else:
        print(f"  ⚠️  轻微提升 ({total_improvement*100:.1f}%)")

    print("\n注意事项:")
    print("  1. 以上为理论估算,实际效果需回测验证")
    print("  2. 假设状态识别准确率 70%,实际可能更高或更低")
    print("  3. 未考虑市场环境变化的影响")
    print("  4. 交易成本估算基于经验值,实际可能不同")

    return total_improvement

def design_backtest_plan():
    """设计回测验证方案."""
    print("\n" + "="*100)
    print("回测验证方案")
    print("="*100)

    print("\n方案 A: 快速验证 (推荐)")
    print("-" * 100)
    print("\n目标: 快速对比两个版本的核心指标")
    print("\n步骤:")
    print("  1. 使用相同的策略配置")
    print("  2. 相同的回测时间段: 2024-07-01 ~ 2026-03-03")
    print("  3. 对比指标:")
    print("     - 年化收益率")
    print("     - Sharpe 比率")
    print("     - 最大回撤")
    print("     - 胜率")
    print("     - 换手率")
    print("\n预计时间: 30 分钟")

    print("\n方案 B: 完整验证")
    print("-" * 100)
    print("\n目标: 全面评估模型改进效果")
    print("\n步骤:")
    print("  1. 多时间段回测:")
    print("     - 牛市: 2024-07-01 ~ 2024-12-31")
    print("     - 震荡: 2025-01-01 ~ 2025-06-30")
    print("     - 熊市: 2025-07-01 ~ 2026-03-03")
    print("  2. 多策略对比:")
    print("     - 纯因子策略 (baseline)")
    print("     - 旧版本 HMM")
    print("     - 新版本 HMM")
    print("  3. 分行业分析:")
    print("     - 哪些行业改进明显")
    print("     - 哪些行业效果不佳")
    print("\n预计时间: 2-3 小时")

    print("\n方案 C: 实盘模拟")
    print("-" * 100)
    print("\n目标: 模拟实盘环境验证")
    print("\n步骤:")
    print("  1. Paper Trading 模式")
    print("  2. 并行运行两个版本")
    print("  3. 实时对比收益曲线")
    print("  4. 持续 1-2 周")
    print("\n预计时间: 1-2 周")

    print("\n推荐方案: 先执行方案 A,如果效果显著再执行方案 B")

def generate_backtest_script():
    """生成回测脚本."""
    script = '''#!/usr/bin/env python
"""HMM 模型版本对比回测脚本."""
import sys
import json
from datetime import date

# 配置
OLD_MODEL_PATH = "backend/data/hmm_models/564b407f-1541-4b18-a087-2a45cfbca9d9/2026-04-04/models.json"
OLD_COEFF_PATH = "backend/data/hmm_models/564b407f-1541-4b18-a087-2a45cfbca9d9/2026-04-04/coefficients_preset_A_2024-07-01_2026-03-03.json"

NEW_MODEL_PATH = "backend/data/hmm_models/b2d5bcc6-8463-4156-bf1a-e1392a00279a/2026-04-27/models.json"
NEW_COEFF_PATH = "backend/data/hmm_models/b2d5bcc6-8463-4156-bf1a-e1392a00279a/2026-04-27/coefficients_preset_A_2024-07-01_2026-03-03.json"

BACKTEST_START = "2024-07-01"
BACKTEST_END = "2026-03-03"

def run_backtest(model_path, coeff_path, label):
    """运行回测."""
    print(f"\\n{'='*80}")
    print(f"回测: {label}")
    print(f"{'='*80}")

    # TODO: 调用 QE 回测引擎
    # 这里需要集成实际的回测逻辑

    print(f"  模型: {model_path}")
    print(f"  系数: {coeff_path}")
    print(f"  时间: {BACKTEST_START} ~ {BACKTEST_END}")
    print("\\n  [待实现] 调用回测引擎...")

    return {
        "label": label,
        "sharpe": 0.0,  # 待填充
        "annual_return": 0.0,
        "max_drawdown": 0.0,
        "win_rate": 0.0,
        "turnover": 0.0,
    }

def compare_results(old_result, new_result):
    """对比回测结果."""
    print(f"\\n{'='*80}")
    print("回测结果对比")
    print(f"{'='*80}")

    print(f"\\n{'指标':20s} | {'旧版本':12s} | {'新版本':12s} | {'提升':12s}")
    print("-" * 65)

    metrics = [
        ("Sharpe 比率", "sharpe", "%"),
        ("年化收益", "annual_return", "%"),
        ("最大回撤", "max_drawdown", "%"),
        ("胜率", "win_rate", "%"),
        ("年化换手", "turnover", "x"),
    ]

    for name, key, unit in metrics:
        old_val = old_result[key]
        new_val = new_result[key]
        delta = new_val - old_val

        if unit == "%":
            print(f"{name:20s} | {old_val:10.2f}% | {new_val:10.2f}% | {delta:+10.2f}%")
        else:
            print(f"{name:20s} | {old_val:12.2f} | {new_val:12.2f} | {delta:+12.2f}")

if __name__ == "__main__":
    print("HMM 模型版本对比回测")

    # 运行回测
    old_result = run_backtest(OLD_MODEL_PATH, OLD_COEFF_PATH, "旧版本 (w3_raw)")
    new_result = run_backtest(NEW_MODEL_PATH, NEW_COEFF_PATH, "新版本 (w5_zscore)")

    # 对比结果
    compare_results(old_result, new_result)
'''

    with open('/f/Dev/AIstock/scripts/backtest_hmm_comparison.py', 'w', encoding='utf-8') as f:
        f.write(script)

    print("\n✅ 回测脚本已生成: scripts/backtest_hmm_comparison.py")

if __name__ == "__main__":
    # 检查是否有新版本的系数文件
    new_coeff_path = "backend/data/hmm_models/b2d5bcc6-8463-4156-bf1a-e1392a00279a/2026-04-27/coefficients_preset_A_2024-07-01_2026-03-03.json"

    import os
    if not os.path.exists(new_coeff_path):
        print("⚠️  新版本系数文件尚未生成")
        print("   需要先触发系数预计算")
        print("\n请执行:")
        print("  1. 等待系数预计算��成 (自动触发)")
        print("  2. 或手动触发系数计算")
    else:
        # 加载系数文件
        old_coeff = load_coefficients(
            "backend/data/hmm_models/564b407f-1541-4b18-a087-2a45cfbca9d9/2026-04-04/coefficients_preset_A_2024-07-01_2026-03-03.json"
        )
        new_coeff = load_coefficients(new_coeff_path)

        # 分析分布
        old_dist = analyze_coefficient_distribution(old_coeff['daily_coefficients'])
        new_dist = analyze_coefficient_distribution(new_coeff['daily_coefficients'])

        # 估算提升
        improvement = estimate_theoretical_improvement(old_dist, new_dist)

    # 设计回测方案
    design_backtest_plan()

    # 生成回测脚本
    generate_backtest_script()
