import shutil
from pathlib import Path
from datetime import datetime

BIN_PROVIDER = "F:/Dev/AIstock/qlib_bin/qlib_bin_20251209"
INSTRUMENTS_DIR = Path(BIN_PROVIDER) / "instruments"
ALL_TXT = INSTRUMENTS_DIR / "all.txt"
ALL_UPDATED_TXT = INSTRUMENTS_DIR / "all_updated.txt"
FEATURES_DIR = Path(BIN_PROVIDER) / "features"
BACKUP_DIR = Path(BIN_PROVIDER).parent / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def main() -> None:
    print("=== 备份现有 all.txt 和 bin 目录 ===")

    # 创建备份目录
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"备份目录: {BACKUP_DIR}")

    # 备份 all.txt
    if ALL_TXT.exists():
        shutil.copy2(ALL_TXT, BACKUP_DIR / "all.txt")
        print(f"已备份 all.txt 到 {BACKUP_DIR / 'all.txt'}")

    # 备份 features 目录
    if FEATURES_DIR.exists():
        shutil.copytree(FEATURES_DIR, BACKUP_DIR / "features")
        print(f"已备份 features 目录到 {BACKUP_DIR / 'features'}")

    print("\n=== 替换 all.txt 为 all_updated.txt ===")

    if not ALL_UPDATED_TXT.exists():
        print(f"错误: {ALL_UPDATED_TXT} 不存在，请先运行扫描脚本")
        return

    shutil.copy2(ALL_UPDATED_TXT, ALL_TXT)
    print(f"已将 {ALL_UPDATED_TXT} 复制到 {ALL_TXT}")

    print(f"\n备份完成！备份位置: {BACKUP_DIR}")
    print("下一步: 运行 regenerate_bin.py 重新生成 bin 文件")

if __name__ == "__main__":
    main()
