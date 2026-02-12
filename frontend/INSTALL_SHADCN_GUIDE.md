# Shadcn/UI 组件安装指南

## 问题说明

AIstock前端项目需要安装shadcn/ui组件库来支持RDagent模型配置页面。

## 安装步骤

### 方法1：使用批处理脚本（推荐）

1. 打开PowerShell，激活AIstock环境：
```powershell
conda activate AIstock
cd F:\Dev\AIstock\frontend
```

2. 执行安装脚本：
```powershell
.\install_shadcn_components.bat
```

### 方法2：手动安装

在PowerShell中，激活AIstock环境后逐个安装：

```powershell
conda activate AIstock
cd F:\Dev\AIstock\frontend

# 使用新的shadcn命令（不是shadcn-ui）
npx shadcn@latest add card
npx shadcn@latest add button
npx shadcn@latest add badge
npx shadcn@latest add dialog
npx shadcn@latest add input
npx shadcn@latest add label
npx shadcn@latest add select
npx shadcn@latest add textarea
npx shadcn@latest add tabs
npx shadcn@latest add alert
```

## 注意事项

1. **必须使用 `shadcn@latest`，不是 `shadcn-ui@latest`**
   - `shadcn-ui` 包已废弃
   - 新版本使用 `shadcn` 包

2. **必须在AIstock conda环境中执行**
   - 不要在base环境或其他环境中安装
   - 确保先执行 `conda activate AIstock`

3. **lucide-react已安装**
   - 项目已包含 `lucide-react@0.563.0`
   - 无需重复安装

## 验证安装

安装完成后，检查是否生成了以下目录：

```
F:\Dev\AIstock\frontend\src\components\ui\
```

该目录下应包含：
- card.tsx
- button.tsx
- badge.tsx
- dialog.tsx
- input.tsx
- label.tsx
- select.tsx
- textarea.tsx
- tabs.tsx
- alert.tsx

## 启动前端

```powershell
conda activate AIstock
cd F:\Dev\AIstock\frontend
npm run dev
```

访问：`http://localhost:3000/config/rdagent-llm`

## 导航链接已添加

导航栏"RD-Agent管理"分组下已添加：
- 🤖 RDagent 模型配置

## 故障排查

### 问题1：命令未找到

如果提示 `npx: command not found`，检查：
```powershell
node --version
npm --version
```

如果未安装Node.js，需要在conda环境中安装：
```powershell
conda install -c conda-forge nodejs
```

### 问题2：组件未生成

如果执行后没有生成组件文件，可能需要先初始化shadcn配置：
```powershell
npx shadcn@latest init
```

按提示选择：
- TypeScript: Yes
- Style: Default
- Base color: Slate
- CSS variables: Yes
- 其他选项使用默认值

### 问题3：导入错误

如果前端报错 `Cannot find module '@/components/ui/card'`，检查：
1. 组件文件是否已生成
2. tsconfig.json中是否配置了路径别名
3. 重启开发服务器

## 完成后的文件结构

```
F:\Dev\AIstock\frontend\
├── src\
│   ├── app\
│   │   └── config\
│   │       └── rdagent-llm\
│   │           ├── page.tsx
│   │           └── components\
│   │               ├── ProviderCard.tsx
│   │               ├── ModelItem.tsx
│   │               ├── AddModelDialog.tsx
│   │               ├── StageMappingConfig.tsx
│   │               ├── CurrentConfigDisplay.tsx
│   │               └── ChangeLogViewer.tsx
│   └── components\
│       └── ui\              # shadcn组件目录
│           ├── card.tsx
│           ├── button.tsx
│           ├── badge.tsx
│           └── ...
├── package.json
└── install_shadcn_components.bat
```
