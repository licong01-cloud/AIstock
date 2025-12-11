import os
import sys
import json
import textwrap
from datetime import datetime

import requests

# ===========================
# 配置区：如无特殊需要可直接使用
# ===========================
# 后端地址
BACKEND_BASE = os.environ.get("TDX_BACKEND_BASE", "http://127.0.0.1:8001")

# qlib bin 根目录 & Snapshot ID（你的目标目录：qlib_bin\\qlib_bin_20251209）
QLIB_BIN_ROOT = r"C:\\Users\\lc999\\NewAIstock\\AIstock\\qlib_bin"
SNAPSHOT_ID = "qlib_bin_20251209"

# 指数与日期区间
INDEX_CODE = "000300.SH"
START_DATE = "2010-01-07"
END_DATE = "2025-12-01"


def backend_url(path: str) -> str:
    return BACKEND_BASE.rstrip("/") + path


def export_index_bin():
    print("=== 1) 导出指数到 qlib bin ===")
    url = backend_url("/api/v1/qlib/index/bin/export")
    payload = {
        "snapshot_id": SNAPSHOT_ID,
        "index_code": INDEX_CODE,
        "start": START_DATE,
        "end": END_DATE,
        "run_health_check": True,
    }
    print("POST", url)
    print("Payload:", payload)

    resp = requests.post(url, json=payload, timeout=600)
    if not resp.ok:
        print("HTTP", resp.status_code, resp.reason)
        print("Body:", resp.text)
        raise SystemExit("导出接口调用失败")

    data = resp.json()
    print("导出返回：")
    print(json.dumps(data, ensure_ascii=False, indent=2))

    if not data.get("dump_bin_ok", False):
        print("WARNING: dump_bin_ok = False，dump_bin 失败，建议检查 stdout/stderr")
    else:
        print("dump_bin_ok = True")

    check_ok = data.get("check_ok")
    if check_ok is not None:
        print("健康检查结果(check_ok):", check_ok)

    stdout_dump = (data.get("stdout_dump") or "").strip()
    stderr_dump = (data.get("stderr_dump") or "").strip()
    if stdout_dump:
        print("\n[dump_bin stdout] 前 20 行：")
        print("\n".join(stdout_dump.splitlines()[:20]))
    if stderr_dump:
        print("\n[dump_bin stderr] 前 20 行：")
        print("\n".join(stderr_dump.splitlines()[:20]))

    return data


def health_check_index():
    print("\n=== 2) 指数 bin 健康检查接口 ===")
    url = backend_url("/api/v1/qlib/index/health_check")
    payload = {"snapshot_id": SNAPSHOT_ID}
    print("POST", url)
    print("Payload:", payload)

    resp = requests.post(url, json=payload, timeout=600)
    if not resp.ok:
        print("HTTP", resp.status_code, resp.reason)
        print("Body:", resp.text)
        raise SystemExit("指数健康检查接口调用失败")

    data = resp.json()
    print("健康检查返回：")
    print(json.dumps(data, ensure_ascii=False, indent=2))

    if not data.get("has_index_file", False):
        raise SystemExit("ERROR: instruments/index.txt 不存在或为空")

    print(f"instruments/index.txt 存在，指数数目 = {data.get('index_count')}")
    if data.get("check_ok") is False:
        print("WARNING: check_data_health.py 报告存在问题，请查看 stdout/stderr 详细信息")

    return data


def check_qlib_format():
    print("\n=== 3) 使用 qlib 检查 bin 文件格式 ===")

    # 延迟导入 qlib 及其依赖，如果本机环境未安装则只给出提示并跳过本地格式检查
    try:
        import qlib  # type: ignore
        from qlib.data import D  # type: ignore
    except Exception as exc:  # noqa: BLE001
        print("本地未安装完整的 qlib 依赖，跳过 bin 文件格式检查。具体原因:")
        print(repr(exc))
        print(
            "前两步（导出 + 后端健康检查）已执行完毕，如需在本机用 qlib 做格式校验，\n"
            "请在当前 Python 环境中安装 qlib 及其依赖（例如 ruamel.yaml）后重新运行本函数。"
        )
        return

    qlib_dir = os.path.join(QLIB_BIN_ROOT, SNAPSHOT_ID)
    print("使用 qlib_dir:", qlib_dir)

    if not os.path.isdir(qlib_dir):
        raise SystemExit(f"ERROR: qlib_dir 不存在: {qlib_dir}")

    qlib.init(
        provider_uri=qlib_dir,
        region="cn",
    )

    # 1) 检查 index instruments 注册
    idx_insts = D.instruments("index")
    print("Index instruments count:", len(idx_insts))
    print("First 10 instruments:", idx_insts[:10])

    if INDEX_CODE not in idx_insts:
        raise SystemExit(f"ERROR: {INDEX_CODE} 不在 instruments('index') 中")

    # 2) 读取 000300.SH 的特征，检查 MultiIndex & 字段名
    df = D.features(
        [INDEX_CODE],
        ["$open", "$high", "$low", "$close", "$volume"],
        start_time=START_DATE,
        end_time=END_DATE,
    )

    print("Data shape:", df.shape)
    print("Index names:", df.index.names)
    print("Columns:", list(df.columns))

    # 基本格式校验
    assert df.index.nlevels == 2, "Index 应为 MultiIndex (datetime, instrument)"
    assert df.index.names[0] == "datetime", "第一个索引层应为 'datetime'"
    assert df.index.names[1] == "instrument", "第二个索引层应为 'instrument'"

    required_cols = ["$open", "$high", "$low", "$close", "$volume"]
    for c in required_cols:
        if c not in df.columns:
            raise SystemExit(f"ERROR: 缺少列 {c}")

    dt_index = df.index.get_level_values("datetime")
    if len(dt_index) > 0:
        print("Date range:", dt_index[0], "->", dt_index[-1])

    print("✅ Qlib bin 格式检查通过。")


def main():
    print(
        textwrap.dedent(
            f"""
            === Qlib 指数 bin 导出 & 检查脚本 ===
            后端: {BACKEND_BASE}
            Snapshot ID: {SNAPSHOT_ID}
            Index: {INDEX_CODE}
            日期: {START_DATE} ~ {END_DATE}
            目标 qlib_dir: {os.path.join(QLIB_BIN_ROOT, SNAPSHOT_ID)}
            时间: {datetime.now().isoformat()}
            """
        )
    )

    export_index_bin()
    health_check_index()
    check_qlib_format()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("脚本执行异常:", repr(e))
        sys.exit(1)
