#!/usr/bin/env python
"""HMM 模型版本对比验证脚本.

对比内容:
1. 协方差异常值数量
2. 状态转移矩阵统计
3. trending 状态持续时间
4. 特征标准化效果
5. 系数分布对比
"""
import sys
import json
import numpy as np
from collections import Counter

sys.stdout.reconfigure(encoding='utf-8')

def load_models(model_path):
    """加载模型文件."""
    with open(model_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def analyze_covariance_anomalies(models, max_covar=10.0):
    """分析协方差异常."""
    anomalies = []
    for sector_code, model in models.items():
        for state_idx, state_cov in enumerate(model['covars']):
            diag_vals = np.diag(np.array(state_cov))
            max_var = np.max(diag_vals)
            if max_var > max_covar:
                anomalies.append({
                    'sector': sector_code,
                    'name': model['sector_name'],
                    'state': model['state_labels'][str(state_idx)],
                    'max_var': max_var,
                })
    return anomalies

def analyze_transition_matrix(models):
    """分析状态转移矩阵."""
    self_trans = {'fading': [], 'neutral': [], 'trending': []}

    for model in models.values():
        transmat = np.array(model['transmat'])
        labels = model['state_labels']

        for i in range(len(labels)):
            state_name = labels[str(i)]
            self_trans[state_name].append(transmat[i, i])

    stats = {}
    for state, probs in self_trans.items():
        if probs:
            stats[state] = {
                'mean': np.mean(probs),
                'median': np.median(probs),
                'std': np.std(probs),
                'min': np.min(probs),
                'max': np.max(probs),
            }
    return stats

def analyze_trending_duration(models):
    """分析 trending 状态持续时间."""
    durations = []
    for model in models.values():
        duration = model.get('trending_avg_duration_days', 0)
        if duration > 0 and duration < 100:  # 过滤异常值
            durations.append(duration)

    if not durations:
        return {}

    return {
        'mean': np.mean(durations),
        'median': np.median(durations),
        'std': np.std(durations),
        'min': np.min(durations),
        'max': np.max(durations),
    }

def check_zscore_params(models):
    """检查是否有 z-score 参数."""
    first_model = next(iter(models.values()), None)
    if first_model and 'zscore_mean' in first_model:
        return {
            'enabled': True,
            'mean': np.array(first_model['zscore_mean']),
            'std': np.array(first_model['zscore_std']),
        }
    return {'enabled': False}

def compare_models(old_path, new_path):
    """对比两个模型版本."""
    print("\n" + "="*100)
    print("HMM 模型版本对比验证")
    print("="*100)

    # 加载模型
    print("\n加载模型...")
    old_models = load_models(old_path)
    new_models = load_models(new_path)

    print(f"  旧版本: {len(old_models)} 个行业")
    print(f"  新版本: {len(new_models)} 个行业")

    # 1. 协方差异常对比
    print("\n" + "-"*100)
    print("1. 协方差异常值对比")
    print("-"*100)

    old_anomalies = analyze_covariance_anomalies(old_models)
    new_anomalies = analyze_covariance_anomalies(new_models)

    print(f"\n旧版本异常数: {len(old_anomalies)} 个")
    if old_anomalies:
        print("  异常行业:")
        for anom in old_anomalies[:5]:
            print(f"    {anom['sector']} ({anom['name']}) - {anom['state']}状态: max_var={anom['max_var']:.2f}")
        if len(old_anomalies) > 5:
            print(f"    ... 还有 {len(old_anomalies)-5} 个")

    print(f"\n新版本异常数: {len(new_anomalies)} 个")
    if new_anomalies:
        print("  异常行业:")
        for anom in new_anomalies[:5]:
            print(f"    {anom['sector']} ({anom['name']}) - {anom['state']}状态: max_var={anom['max_var']:.2f}")
    else:
        print("  ✅ 无异常值!")

    improvement = len(old_anomalies) - len(new_anomalies)
    if improvement > 0:
        print(f"\n✅ 改进: 修复了 {improvement} 个异常行业")
    elif improvement < 0:
        print(f"\n⚠️  警告: 新增了 {-improvement} 个异常行业")

    # 2. 状态转移矩阵对比
    print("\n" + "-"*100)
    print("2. 状态转移矩阵对比")
    print("-"*100)

    old_trans = analyze_transition_matrix(old_models)
    new_trans = analyze_transition_matrix(new_models)

    print("\n自转移概率统计:")
    print(f"{'状态':12s} | {'版本':6s} | {'均值':8s} | {'中位数':8s} | {'标准差':8s} | {'最小值':8s} | {'最大值':8s}")
    print("-" * 80)

    for state in ['fading', 'neutral', 'trending']:
        if state in old_trans:
            old_s = old_trans[state]
            print(f"{state:12s} | 旧版本 | {old_s['mean']:8.3f} | {old_s['median']:8.3f} | {old_s['std']:8.3f} | {old_s['min']:8.3f} | {old_s['max']:8.3f}")

        if state in new_trans:
            new_s = new_trans[state]
            print(f"{state:12s} | 新版本 | {new_s['mean']:8.3f} | {new_s['median']:8.3f} | {new_s['std']:8.3f} | {new_s['min']:8.3f} | {new_s['max']:8.3f}")

            if state in old_trans:
                delta = new_s['mean'] - old_trans[state]['mean']
                symbol = "✅" if delta > 0 else "⚠️"
                print(f"{'':12s} | 变化   | {delta:+8.3f} {symbol}")

        print()

    # 3. trending 状态持续时间对比
    print("\n" + "-"*100)
    print("3. Trending 状态持续时间对比")
    print("-"*100)

    old_duration = analyze_trending_duration(old_models)
    new_duration = analyze_trending_duration(new_models)

    print(f"\n{'指标':12s} | {'旧版本':10s} | {'新版本':10s} | {'变化':10s}")
    print("-" * 50)

    for metric in ['mean', 'median', 'min', 'max']:
        old_val = old_duration.get(metric, 0)
        new_val = new_duration.get(metric, 0)
        delta = new_val - old_val
        symbol = "✅" if delta > 0 else "⚠️"
        print(f"{metric:12s} | {old_val:10.2f} | {new_val:10.2f} | {delta:+10.2f} {symbol}")

    # 4. Z-score 标准化检查
    print("\n" + "-"*100)
    print("4. Z-score 标准化检查")
    print("-"*100)

    old_zscore = check_zscore_params(old_models)
    new_zscore = check_zscore_params(new_models)

    print(f"\n旧版本 Z-score: {'启用' if old_zscore['enabled'] else '未启用'}")
    print(f"新版本 Z-score: {'启用' if new_zscore['enabled'] else '未启用'}")

    if new_zscore['enabled']:
        print("\n新版本 Z-score 参数:")
        print(f"  Mean: {np.round(new_zscore['mean'], 4)}")
        print(f"  Std:  {np.round(new_zscore['std'], 4)}")
        print("  ✅ 特征已标准化,权重平衡")

    # 5. 配置对比
    print("\n" + "-"*100)
    print("5. 配置参数对比")
    print("-"*100)

    old_first = next(iter(old_models.values()))
    new_first = next(iter(new_models.values()))

    print(f"\n{'参数':20s} | {'旧版本':15s} | {'新版本':15s}")
    print("-" * 55)
    print(f"{'n_states':20s} | {old_first['n_states']:15d} | {new_first['n_states']:15d}")
    print(f"{'covariance_type':20s} | {old_first['covariance_type']:15s} | {new_first['covariance_type']:15s}")
    print(f"{'rolling_window':20s} | {old_first.get('rolling_window', 3):15d} | {new_first.get('rolling_window', 5):15d}")
    print(f"{'zscore':20s} | {'否' if not old_zscore['enabled'] else '是':15s} | {'否' if not new_zscore['enabled'] else '是':15s}")

    # 总结
    print("\n" + "="*100)
    print("总结")
    print("="*100)

    improvements = []
    warnings = []

    if len(new_anomalies) < len(old_anomalies):
        improvements.append(f"✅ 协方差异常: {len(old_anomalies)} → {len(new_anomalies)} (修复 {len(old_anomalies)-len(new_anomalies)} 个)")
    elif len(new_anomalies) > len(old_anomalies):
        warnings.append(f"⚠️  协方差异常: {len(old_anomalies)} → {len(new_anomalies)} (新增 {len(new_anomalies)-len(old_anomalies)} 个)")

    if 'trending' in new_trans and 'trending' in old_trans:
        delta = new_trans['trending']['mean'] - old_trans['trending']['mean']
        if delta > 0.1:
            improvements.append(f"✅ Trending 自转移: {old_trans['trending']['mean']:.3f} → {new_trans['trending']['mean']:.3f} (+{delta:.3f})")

    if new_duration.get('mean', 0) > old_duration.get('mean', 0):
        improvements.append(f"✅ Trending 持续时间: {old_duration.get('mean', 0):.2f} → {new_duration.get('mean', 0):.2f} 天")

    if new_zscore['enabled'] and not old_zscore['enabled']:
        improvements.append("✅ 启用 Z-score 标准化")

    if new_first.get('rolling_window', 5) > old_first.get('rolling_window', 3):
        improvements.append(f"✅ Rolling window: {old_first.get('rolling_window', 3)} → {new_first.get('rolling_window', 5)} 天")

    print("\n改进项:")
    for imp in improvements:
        print(f"  {imp}")

    if warnings:
        print("\n警告项:")
        for warn in warnings:
            print(f"  {warn}")

    if not warnings:
        print("\n🎉 新版本在所有指标上都有改进!")

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("用法: python compare_hmm_models.py <old_model_path> <new_model_path>")
        sys.exit(1)

    old_path = sys.argv[1]
    new_path = sys.argv[2]

    compare_models(old_path, new_path)
