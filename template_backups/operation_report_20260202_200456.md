========================================================================================================================
RD-Agent模板版本管理操作报告
========================================================================================================================

**操作时间**: 2026-02-02 20:04:56

## 一、操作摘要

### ✅ 已完成的操作

1. **逻辑一致性检查**: 通过
   - 分析了41个section
   - 未发现真正的逻辑冲突
   - 阶段划分清晰合理

2. **创建v0-v3备份压缩包**: 完成
   - 文件: F:\Dev\AIstock\template_backups\rdagent_templates_v0-v3_backup_20260202_200441.zip
   - 大小: 188.69 MB

3. **备份当前使用版本**: 完成
   - 目录: F:\Dev\AIstock\template_backups\current_version_backup_20260202_200456
   - 文件数: 11 个

4. **用当前版本覆盖v0**: 完成
   - 目标: F:\Dev\RD-Agent-main\app_tpl\qlib\v0
   - 覆盖文件: 6 个

5. **更新激活状态**: 完成
   - 激活版本: qlib/v0
   - 状态文件: backend_data/rdagent_active_template.json

## 二、版本说明

### 当前版本特征

- **基于**: v0 (75%文件匹配)
- **主要修改**: 删除了prompts.yaml中的部分约束规则
- **修改时间**: 2026-02-02 17:43
- **文件数**: 6个关键文件

### 新v0版本

- **来源**: 当前正在使用的版本
- **状态**: 已激活
- **用途**: 今后修改的基线版本

## 三、备份文件位置

### 完整备份

```
F:\Dev\AIstock\template_backups\rdagent_templates_v0-v3_backup_20260202_200441.zip
```

**包含内容**:
- v0: qlib/v0 (原始v0版本)
- v1: all/v1
- v2: all/v2
- v3: all/V3

### 当前版本备份

```
F:\Dev\AIstock\template_backups\current_version_backup_20260202_200456
```

## 四、后续使用指南

### 1. 基于v0进行修改

今后所有模板修改应基于v0版本:

```bash
# v0路径
F:\Dev\RD-Agent-main\app_tpl\qlib\v0
```

### 2. 回滚操作

如需回滚到之前的版本:

**方法1: 使用完整备份**
```bash
# 解压备份文件
unzip rdagent_templates_v0-v3_backup_20260202_200441.zip
```

**方法2: 使用API回滚**
```bash
# 查看备份列表
curl http://localhost:8001/api/v1/rdagent/templates/backups

# 回滚到指定备份
curl -X POST http://localhost:8001/api/v1/rdagent/templates/rollback?backup_id=BACKUP_ID
```

### 3. 创建新版本

基于v0创建新版本(如v4):

```bash
# 1. 复制v0到新版本目录
cp -r F:\Dev\RD-Agent-main\app_tpl\qlib\v0 F:\Dev\RD-Agent-main\app_tpl/all/v4

# 2. 在v4中进行修改
# 3. 使用API应用新版本
curl -X POST http://localhost:8001/api/v1/rdagent/templates/all/v4/apply
```

## 五、重要提醒

⚠️ **注意事项**:

1. v0现在是基线版本，请勿直接修改v0目录中的文件
2. 创建新版本时，应复制v0后再修改
3. 所有备份文件请妥善保管，以便回滚
4. 使用模板应用API时会自动创建备份

========================================================================================================================
报告生成时间: 2026-02-02T20:04:56.393142
========================================================================================================================