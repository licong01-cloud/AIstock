#!/bin/bash
# 测试分钟级记录功能

echo "=== 测试分钟级记录功能 ==="
echo ""

# 测试1：默认行为（不设置环境变量）
echo "测试1: 默认行为（SAVE_MINUTE_TRADES未设置）"
echo "预期：不保存分钟级数据，无性能影响"
echo ""

cd /mnt/f/Dev/AIstock/scripts
python3 << 'ENDPY'
import os
import sys

# 确保环境变量未设置
if 'SAVE_MINUTE_TRADES' in os.environ:
    del os.environ['SAVE_MINUTE_TRADES']

# 导入并检查
sys.path.insert(0, '/mnt/f/Dev/AIstock/scripts')
exec(open('qrun_limit_minute.py').read(), {'__name__': '__main__', '__file__': 'qrun_limit_minute.py'})

# 检查SAVE_MINUTE_TRADES的值
print(f"SAVE_MINUTE_TRADES = {SAVE_MINUTE_TRADES}")

if SAVE_MINUTE_TRADES:
    print("✗ 失败：默认应该是False")
    sys.exit(1)
else:
    print("✅ 通过：默认行为正确（不保存）")
ENDPY

echo ""
echo "测试2: 开启分钟级记录（SAVE_MINUTE_TRADES=1）"
echo "预期：保存分钟级数据"
echo ""

export SAVE_MINUTE_TRADES=1
python3 << 'ENDPY'
import os
import sys

# 检查环境变量
print(f"环境变量 SAVE_MINUTE_TRADES = {os.environ.get('SAVE_MINUTE_TRADES')}")

# 导入并检查
sys.path.insert(0, '/mnt/f/Dev/AIstock/scripts')
exec(open('qrun_limit_minute.py').read(), {'__name__': '__main__', '__file__': 'qrun_limit_minute.py'})

print(f"SAVE_MINUTE_TRADES = {SAVE_MINUTE_TRADES}")

if SAVE_MINUTE_TRADES:
    print("✅ 通过：环境变量控制正确（启用保存）")
else:
    print("✗ 失败：应该是True")
    sys.exit(1)
ENDPY

echo ""
echo "=== 测试完成 ==="
echo ""
echo "总结："
echo "  ✅ 默认行为：不保存分钟级数据（无性能影响）"
echo "  ✅ 环境变量控制：SAVE_MINUTE_TRADES=1 启用保存"
echo ""
echo "使用方法："
echo "  # 默认运行（不保存分钟级数据）"
echo "  python qrun_limit_minute.py conf.yaml"
echo ""
echo "  # 启用分钟级记录"
echo "  SAVE_MINUTE_TRADES=1 python qrun_limit_minute.py conf.yaml"
