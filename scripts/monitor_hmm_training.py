#!/usr/bin/env python
"""监控 HMM 训练进度并在完成后进行对比验证."""
import sys
import time
import requests
import json
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

# 配置
OLD_CONFIG_ID = "564b407f-1541-4b18-a087-2a45cfbca9d9"
OLD_SNAPSHOT_ID = "252fdd35-aae3-445a-baf4-7e46b1b93aff"
NEW_CONFIG_ID = "b2d5bcc6-8463-4156-bf1a-e1392a00279a"
NEW_JOB_ID = "042c873a-d4ef-40e2-ac90-f9eeef719b8b"
API_BASE = "http://localhost:8001/api/v1"

def check_job_status(config_id, job_id):
    """检查训练任务状态."""
    resp = requests.get(f"{API_BASE}/hmm-training/configs/{config_id}/jobs")
    if resp.status_code != 200:
        return None

    jobs = resp.json()
    for job in jobs:
        if job["job_id"] == job_id:
            return job
    return None

def get_snapshot(snapshot_id):
    """获取快照详情."""
    # 需要通过 service 获取,这里简化处理
    return None

def compare_models(old_snapshot_id, new_snapshot_id):
    """对比两个模型版本."""
    print("\n" + "="*80)
    print("模型对比验证")
    print("="*80)

    # 读取模型文件
    old_path = f"/f/Dev/AIstock/backend/data/hmm_models/{OLD_CONFIG_ID}/2026-04-04/models.json"

    # 新模型路径需要从 snapshot 获取
    print(f"\n旧版本: {OLD_CONFIG_ID}")
    print(f"  快照: {old_snapshot_id}")
    print(f"  路径: {old_path}")

    print(f"\n新版本: {NEW_CONFIG_ID}")
    print(f"  快照: {new_snapshot_id}")
    print(f"  等待训练完成后进行详细对比...")

def main():
    print("HMM 训练监控")
    print("="*80)
    print(f"旧版本: {OLD_CONFIG_ID}")
    print(f"新版本: {NEW_CONFIG_ID}")
    print(f"Job ID: {NEW_JOB_ID}")
    print()

    start_time = datetime.now()
    check_interval = 30  # 30 秒检查一次

    while True:
        job = check_job_status(NEW_CONFIG_ID, NEW_JOB_ID)
        if job is None:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 无法获取任务状态")
            time.sleep(check_interval)
            continue

        status = job["status"]
        elapsed = (datetime.now() - start_time).total_seconds() / 60

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 状态: {status:10s} | 已用时: {elapsed:.1f} 分钟", end="")

        if status == "completed":
            print("\n\n✅ 训练完成!")
            snapshot_id = job.get("snapshot_id")
            if snapshot_id:
                print(f"  Snapshot ID: {snapshot_id}")
                compare_models(OLD_SNAPSHOT_ID, snapshot_id)
            break
        elif status == "failed":
            print("\n\n❌ 训练失败!")
            error_msg = job.get("error_message", "未知错误")
            print(f"  错误信息: {error_msg}")
            break
        elif status == "running":
            print(" (训练中...)")
        else:
            print()

        time.sleep(check_interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n监控已停止")
