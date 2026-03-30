"""分钟线全量回测端到端验证
直接使用 RDAgent conf_baseline.yaml + qrun_limit.py 配置，
验证 NestedExecutor + TWAP + limit_up/limit_down 在完整测试期(2024-07~2026-03)的正确性和性能。

在 WSL rdagent-gpu 环境中执行:
  python /mnt/f/Dev/AIstock/scripts/validate_minute_fullperiod.py
"""
import os
import sys
import time
import shutil
import tempfile
import subprocess
from pathlib import Path

RDAGENT_ROOT = Path("/mnt/f/Dev/RD-Agent-main")
FACTOR_TPL = RDAGENT_ROOT / "rdagent/scenarios/qlib/experiment/factor_template"

def main():
    # 创建临时工作目录
    workdir = Path(tempfile.mkdtemp(prefix="minute_validate_"))
    print(f"=== 分钟线全量回测验证 ===")
    print(f"工作目录: {workdir}")
    print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 复制配置文件
    shutil.copy2(FACTOR_TPL / "conf_baseline.yaml", workdir / "conf.yaml")

    # 创建修复版 qrun_limit.py — 完整 workflow: YAML→patch→init→train→backtest
    qrun_code = '''"""qrun_limit wrapper — 完整 Qlib workflow with limit_threshold patching"""
import os
import sys
from pathlib import Path
from ruamel.yaml import YAML
import qlib
from qlib.config import C
from qlib.workflow.cli import sys_config, task_train

def patch_backtest_config(config):
    if isinstance(config, dict):
        for key, val in config.items():
            if key == "exchange_kwargs" and isinstance(val, dict):
                lt = val.get("limit_threshold")
                if isinstance(lt, list):
                    val["limit_threshold"] = tuple(lt)
            elif key == "backtest" and isinstance(val, dict):
                patch_backtest_config(val)
            else:
                patch_backtest_config(val)
    elif isinstance(config, list):
        for item in config:
            patch_backtest_config(item)

def main():
    yaml_path = sys.argv[1] if len(sys.argv) > 1 else "conf.yaml"
    yaml = YAML(typ="safe", pure=True)
    config = yaml.load(Path(yaml_path))
    patch_backtest_config(config)
    sys_config(config, config_path=yaml_path)

    # Init qlib with exp_manager pointing to local mlruns
    exp_manager = C["exp_manager"]
    exp_manager["kwargs"]["uri"] = "file:" + str(Path(os.getcwd()).resolve() / "mlruns")
    qlib.init(**config.get("qlib_init"), exp_manager=exp_manager)

    # Run training + backtesting
    experiment_name = config.get("experiment_name", "workflow")
    recorder = task_train(config.get("task"), experiment_name=experiment_name)
    recorder.save_objects(config=config)
    print("WORKFLOW_COMPLETE")

if __name__ == "__main__":
    main()
'''
    (workdir / "qrun_limit.py").write_text(qrun_code)

    # ===== Phase 1: 数据可达性验证 =====
    print("=== Phase 1: 数据可达性验证 ===")
    import qlib
    from qlib.data import D

    t0 = time.time()
    qlib.init(provider_uri={
        "day": "/home/lc999/data/qlib_bin",
        "1min": "/home/lc999/data/qlib_minute_bin",
    }, region="cn", dataset_cache=None, expression_cache=None)
    print(f"  Qlib 初始化: {time.time()-t0:.1f}s")

    # 日线 calendar
    cal_day = D.calendar(start_time="2024-07-01", end_time="2026-03-09", freq="day")
    print(f"  日线 calendar: {len(cal_day)} 天 ({cal_day[0].strftime('%Y-%m-%d')} ~ {cal_day[-1].strftime('%Y-%m-%d')})")

    # 分钟线 calendar
    cal_min = D.calendar(start_time="2024-07-01", end_time="2026-03-09", freq="1min")
    print(f"  分钟线 calendar: {len(cal_min)} bars")
    expected_min_bars = len(cal_day) * 240
    ratio = len(cal_min) / expected_min_bars if expected_min_bars > 0 else 0
    print(f"  预期 bars (240/天): ~{expected_min_bars:,}, 实际/预期比: {ratio:.3f}")

    # 股票数量
    instruments = D.instruments("all")
    inst_day = D.list_instruments(instruments=instruments, as_list=True, freq="day")
    inst_min = D.list_instruments(instruments=instruments, as_list=True, freq="1min")
    print(f"  日线股票数: {len(inst_day)}, 分钟线股票数: {len(inst_min)}")

    # 抽样验证分钟线数据
    sample_stocks = ["600519.SH", "000001.SZ", "300750.SZ", "601318.SH", "002594.SZ"]
    print(f"\n  抽样验证分钟线数据 ({len(sample_stocks)} 只, 2024-07 一个月):")
    for stock in sample_stocks:
        try:
            df = D.features([stock], ["$close", "$limit_up", "$limit_down"],
                           start_time="2024-07-01", end_time="2024-07-31", freq="1min")
            n_bars = len(df)
            n_limit_up = int((df["$limit_up"] == 1).sum())
            n_limit_down = int((df["$limit_down"] == 1).sum())
            print(f"    {stock}: {n_bars} bars, limit_up={n_limit_up}, limit_down={n_limit_down}")
        except Exception as e:
            print(f"    {stock}: ERROR - {e}")

    # 边界日期检查 — 用范围而非单日
    print(f"\n  边界区间数据检查:")
    for label, start, end in [
        ("回测起始周", "2024-07-01", "2024-07-05"),
        ("回测结束周", "2026-03-03", "2026-03-09"),
    ]:
        try:
            df = D.features(["600519.SH"], ["$close", "$volume"],
                           start_time=start, end_time=end, freq="1min")
            n_bars = len(df)
            if n_bars > 0:
                print(f"    {label} ({start}~{end}): {n_bars} bars, "
                      f"close [{df['$close'].min():.2f}, {df['$close'].max():.2f}]")
            else:
                print(f"    {label} ({start}~{end}): 0 bars (无数据!)")
        except Exception as e:
            print(f"    {label}: ERROR - {e}")

    # 涨跌停覆盖率检查
    print(f"\n  涨跌停覆盖率 (全期 5 只样本股):")
    for stock in sample_stocks[:3]:
        try:
            df = D.features([stock], ["$limit_up", "$limit_down"],
                           start_time="2024-07-01", end_time="2026-03-09", freq="1min")
            total = len(df)
            lu = int((df["$limit_up"] == 1).sum())
            ld = int((df["$limit_down"] == 1).sum())
            lu_pct = lu / total * 100 if total > 0 else 0
            ld_pct = ld / total * 100 if total > 0 else 0
            print(f"    {stock}: {total} bars, 涨停={lu} ({lu_pct:.2f}%), 跌停={ld} ({ld_pct:.2f}%)")
        except Exception as e:
            print(f"    {stock}: ERROR - {e}")

    t_phase1 = time.time() - t0
    print(f"\n  Phase 1 耗时: {t_phase1:.1f}s")

    # ===== Phase 2: 完整 Qlib workflow 回测 (子进程) =====
    print(f"\n{'='*60}")
    print(f"=== Phase 2: 完整 Qlib workflow 回测 ===")
    print(f"  配置: LGBModel + Alpha158(20特征) + NestedExecutor + TWAP")
    print(f"  训练: 2018-08-01 ~ 2022-12-31 (日线)")
    print(f"  回测: 2024-07-01 ~ 2026-03-09 (分钟线执行)")
    print(f"  开始时间: {time.strftime('%H:%M:%S')}")
    print(f"  执行方式: python qrun_limit.py conf.yaml (与 RDAgent 完全一致)")
    sys.stdout.flush()

    t1 = time.time()
    result = subprocess.run(
        ["python", "qrun_limit.py", "conf.yaml"],
        cwd=str(workdir),
        capture_output=True,
        text=True,
        timeout=3600,  # 1小时超时
    )
    t2 = time.time()
    elapsed = t2 - t1

    print(f"\n  退出码: {result.returncode}")
    print(f"  训练+回测总耗时: {elapsed:.1f}s ({elapsed/60:.1f} 分钟)")

    # 打印 stdout (过滤关键行)
    if result.stdout:
        lines = result.stdout.strip().split("\n")
        print(f"\n  === stdout ({len(lines)} 行，关键摘要) ===")
        keywords = ["IC", "ICIR", "Rank", "annualized", "max_drawdown", "Sharpe",
                     "excess", "return", "Training", "LightGBM", "epoch", "valid",
                     "total", "backtest", "ERROR", "WARNING", "portfolio"]
        for line in lines:
            line_lower = line.lower()
            if any(kw.lower() in line_lower for kw in keywords):
                print(f"  | {line}")
        # 最后 20 行总是打印
        print(f"\n  === stdout 尾部 (最后 20 行) ===")
        for line in lines[-20:]:
            print(f"  | {line}")

    if result.stderr:
        lines = result.stderr.strip().split("\n")
        # 过滤 INFO 日志，只显示 WARNING/ERROR
        important = [l for l in lines if "WARNING" in l or "ERROR" in l or "Exception" in l or "Traceback" in l]
        if important:
            print(f"\n  === stderr 重要信息 ({len(important)} 行) ===")
            for line in important[:30]:
                print(f"  | {line}")
        else:
            print(f"\n  stderr: {len(lines)} 行 (全部为 INFO 日志)")

    if result.returncode != 0:
        print(f"\n  回测失败! stderr 尾部:")
        for line in (result.stderr or "").strip().split("\n")[-30:]:
            print(f"  | {line}")
        sys.exit(1)

    # ===== Phase 3: 检查 mlruns 结果 =====
    print(f"\n{'='*60}")
    print(f"=== Phase 3: 回测结果分析 ===")

    mlruns = workdir / "mlruns"
    if mlruns.exists():
        # 列出实验
        for exp_dir in sorted(mlruns.iterdir()):
            if exp_dir.is_dir() and exp_dir.name != ".trash":
                for run_dir in sorted(exp_dir.iterdir()):
                    metrics_dir = run_dir / "metrics"
                    if metrics_dir.exists():
                        print(f"  实验: {exp_dir.name}/{run_dir.name}")
                        for mf in sorted(metrics_dir.iterdir()):
                            try:
                                val = mf.read_text().strip().split()[-1]
                                print(f"    {mf.name}: {val}")
                            except:
                                pass
    else:
        print(f"  mlruns 目录不存在，尝试从 stdout 提取结果")

    # ===== 总结 =====
    t_total = time.time() - t0
    print(f"\n{'='*60}")
    print(f"=== 验证总结 ===")
    print(f"  Phase 1 (数据检查): {t_phase1:.1f}s")
    print(f"  Phase 2 (训练+回测): {elapsed:.1f}s ({elapsed/60:.1f} 分钟)")
    print(f"  总耗时: {t_total:.1f}s ({t_total/60:.1f} 分钟)")
    print(f"  工作目录: {workdir}")
    print(f"  完成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  状态: {'PASS' if result.returncode == 0 else 'FAIL'}")


if __name__ == "__main__":
    main()
