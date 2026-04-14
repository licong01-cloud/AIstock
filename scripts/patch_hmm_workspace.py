"""一次性修补脚本：为已有 workspace 生成 hmm_sector_coefficients.json 并注入 conf.yaml"""
import json, subprocess, sys, os, yaml

WORKSPACE = r"F:\Dev\RD-Agent-main\qe_workspace\qe_20260408_142206"
conf_path = os.path.join(WORKSPACE, "conf.yaml")

with open(conf_path, "r", encoding="utf-8") as f:
    conf = yaml.safe_load(f)

# 从 conf.yaml 提取 HMM 参数
strat_kwargs = conf["task"]["record"][2]["kwargs"]["config"]["strategy"]["kwargs"]
model_path_win = strat_kwargs["sector_hmm_model_path"]
preset_key = strat_kwargs.get("hmm_signal_preset", "preset_A")
presets = strat_kwargs.get("hmm_signal_presets", {})

test_start = conf["task"]["dataset"]["kwargs"]["segments"]["test"][0]
backtest_cfg = conf["task"]["record"][2]["kwargs"]["config"]["backtest"]
backtest_end = backtest_cfg.get("end_time", "2025-12-31")

# Windows -> WSL path
mp = model_path_win.replace("\\", "/")
wsl_model_path = "/mnt/" + mp[0].lower() + mp[2:]
model_dir = wsl_model_path.rsplit("/", 1)[0]

# 尝试读取预计算文件
coeff_filename = f"coefficients_{preset_key}_{test_start}_{backtest_end}.json"
coeff_path = f"{model_dir}/{coeff_filename}"
print(f"Trying cached: {coeff_path}")

result = subprocess.run(
    ["wsl", "bash", "-c", f"cat '{coeff_path}'"],
    capture_output=True, text=True, timeout=10,
    encoding="utf-8", errors="replace",
)

hmm_json = None
if result.returncode == 0 and result.stdout.strip():
    data = json.loads(result.stdout)
    if "daily_coefficients" in data and "stock_sector_map" in data:
        hmm_json = result.stdout.strip()
        print(f"Cache HIT: {len(data['daily_coefficients'])} days, {data.get('sector_count', '?')} sectors")

if not hmm_json:
    # 列出可用文件
    ls = subprocess.run(
        ["wsl", "bash", "-c", f"ls '{model_dir}'/coefficients_*.json 2>/dev/null"],
        capture_output=True, text=True, timeout=10,
    )
    avail = ls.stdout.strip()
    if avail:
        # 用第一个匹配的文件
        first = avail.split("\n")[0]
        print(f"Cache MISS, using fallback: {first}")
        r2 = subprocess.run(
            ["wsl", "bash", "-c", f"cat '{first}'"],
            capture_output=True, text=True, timeout=10,
        )
        if r2.returncode == 0:
            hmm_json = r2.stdout.strip()
    else:
        print(f"No cached files in {model_dir}, need real-time compute")
        sys.exit(1)

# 写入 workspace
out = os.path.join(WORKSPACE, "hmm_sector_coefficients.json")
with open(out, "w", encoding="utf-8") as f:
    f.write(hmm_json)
print(f"Written: {out}")

# 注入 hmm_coefficients_file 到 conf.yaml
if "hmm_coefficients_file" not in strat_kwargs:
    strat_kwargs["hmm_coefficients_file"] = "hmm_sector_coefficients.json"
    with open(conf_path, "w", encoding="utf-8") as f:
        yaml.dump(conf, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    print("conf.yaml updated: added hmm_coefficients_file")
else:
    print("conf.yaml already has hmm_coefficients_file")

print("DONE - experiment is ready to re-run")
