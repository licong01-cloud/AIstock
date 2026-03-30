import json, time, urllib.request, re, os

API = "http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations"

def fetch(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())

# 轮询等待完成
while True:
    try:
        st = fetch(f"{API}/status")
        p = st.get("progress", {})
        if p.get("status") not in ("computing",):
            break
    except:
        pass
    time.sleep(30)

# 获取日志
logs = fetch(f"{API}/logs?after_index=-1")
entries = logs.get("entries", [])

# 分析
success = []
fails = []
for e in entries:
    msg = e["msg"]
    m_ok = re.search(r'\[(\d+)/(\d+)\] (.+?) 缓存生成成功 \(([0-9.]+)s\)', msg)
    m_fail = re.search(r'\[(\d+)/(\d+)\] (.+?) 缓存生成失败: (.+?) \(([0-9.]+)s\)', msg)
    if m_ok:
        success.append({"name": m_ok.group(3), "time": float(m_ok.group(4))})
    elif m_fail:
        fails.append({"name": m_fail.group(3), "reason": m_fail.group(4), "time": float(m_fail.group(5))})

# 最终状态
final_status = fetch(f"{API}/status")
fp = final_status.get("progress", {})

# 输出报告
report = []
report.append("=" * 60)
report.append("因子相关性计算完成报告")
report.append("=" * 60)
report.append(f"最终状态: {fp.get('status')}")
report.append(f"总耗时: {fp.get('elapsed_sec', 0):.0f}s ({fp.get('elapsed_sec', 0)/60:.1f}min)")
report.append(f"模式: {fp.get('mode')}")
report.append(f"成功: {len(success)} 个因子")
report.append(f"失败: {len(fails)} 个因子")
report.append("")

if fails:
    report.append("--- 失败因子列表 ---")
    for f in fails:
        report.append(f"  {f['name']}: {f['reason']} ({f['time']}s)")
    report.append("")

if success:
    times = sorted([s["time"] for s in success], reverse=True)
    report.append("--- 成功因子耗时统计 ---")
    report.append(f"  最大: {times[0]}s")
    report.append(f"  前5慢: {[round(t,1) for t in times[:5]]}")
    report.append(f"  中位数: {times[len(times)//2]:.1f}s")
    report.append(f"  平均: {sum(times)/len(times):.1f}s")
    report.append(f"  <5s: {sum(1 for t in times if t<5)}/{len(times)}")
    report.append("")

# 查询失败因子的代码路径
if fails:
    report.append("--- 失败因子代码分析 ---")
    for f in fails:
        name = f["name"]
        try:
            import psycopg2
            conn = psycopg2.connect("dbname=aistock")
            cur = conn.cursor()
            cur.execute("SELECT qe_code_path FROM aistock_factor_catalog WHERE factor_name=%s", (name,))
            row = cur.fetchone()
            code_path = row[0] if row else None
            cur.close()
            conn.close()
            if code_path:
                abs_path = os.path.join("F:/Dev/AIstock", code_path)
                if os.path.exists(abs_path):
                    with open(abs_path, "r", encoding="utf-8") as cf:
                        code = cf.read()
                    lines = len(code.split("\n"))
                    # 简单分析
                    has_loop = "for " in code or "while " in code
                    has_rolling = "rolling(" in code
                    has_groupby = "groupby(" in code
                    has_apply = ".apply(" in code
                    has_merge = ".merge(" in code or "pd.merge(" in code
                    report.append(f"\n  [{name}]")
                    report.append(f"    路径: {code_path}")
                    report.append(f"    行数: {lines}")
                    report.append(f"    特征: loop={has_loop} rolling={has_rolling} groupby={has_groupby} apply={has_apply} merge={has_merge}")
                    report.append(f"    失败原因: {f['reason']}")
                    report.append(f"    代码前20行:")
                    for i, line in enumerate(code.split("\n")[:20]):
                        report.append(f"      {i+1:3d}| {line}")
                    report.append(f"    ... (共{lines}行)")
        except Exception as ex:
            report.append(f"  [{name}] 读取代码失败: {ex}")

txt = "\n".join(report)
out_path = "F:/Dev/AIstock/correlation_compute_report.txt"
with open(out_path, "w", encoding="utf-8") as wf:
    wf.write(txt)
print(txt)
