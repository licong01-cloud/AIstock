# AIstock前端 - TailwindCSS + Shadcn/UI 完整安装指南

## 问题说明

AIstock前端项目需要安装TailwindCSS和shadcn/ui组件库来支持RDagent模型配置页面。

## 已完成的配置

✅ 已创建以下配置文件：
- `tailwind.config.js` - TailwindCSS配置
- `postcss.config.js` - PostCSS配置
- `components.json` - Shadcn/UI配置
- `src/lib/utils.ts` - 工具函数
- `src/app/globals.css` - 已添加TailwindCSS指令和CSS变量

✅ 已添加导航链接：
- 在"RD-Agent管理"分组下添加了"🤖 RDagent 模型配置"

## 需要执行的安装步骤

### 步骤1：安装TailwindCSS和相关依赖

在PowerShell中执行：

```powershell
conda activate AIstock
cd F:\Dev\AIstock\frontend

# 安装TailwindCSS和PostCSS
npm install -D tailwindcss postcss autoprefixer

# 安装shadcn/ui所需的依赖
npm install class-variance-authority clsx tailwind-merge
npm install tailwindcss-animate

# 安装Radix UI组件（shadcn/ui的底层依赖）
npm install @radix-ui/react-dialog
npm install @radix-ui/react-label
npm install @radix-ui/react-select
npm install @radix-ui/react-slot
npm install @radix-ui/react-tabs
```

### 步骤2：安装shadcn/ui组件

```powershell
# 确保在AIstock环境中
conda activate AIstock
cd F:\Dev\AIstock\frontend

# 安装所需的shadcn组件
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

### 步骤3：验证安装

检查是否生成了以下目录和文件：

```
F:\Dev\AIstock\frontend\
├── src\
│   ├── components\
│   │   └── ui\          # shadcn组件目录
│   │       ├── card.tsx
│   │       ├── button.tsx
│   │       ├── badge.tsx
│   │       ├── dialog.tsx
│   │       ├── input.tsx
│   │       ├── label.tsx
│   │       ├── select.tsx
│   │       ├── textarea.tsx
│   │       ├── tabs.tsx
│   │       └── alert.tsx
│   └── lib\
│       └── utils.ts     # 已创建
├── tailwind.config.js   # 已创建
├── postcss.config.js    # 已创建
└── components.json      # 已创建
```

### 步骤4：启动前端服务

```powershell
conda activate AIstock
cd F:\Dev\AIstock\frontend
npm run dev
```

访问：`http://localhost:3000/config/rdagent-llm`

## 完整的一键安装命令

将以下命令复制到PowerShell中一次性执行：

```powershell
conda activate AIstock
cd F:\Dev\AIstock\frontend

# 安装TailwindCSS
npm install -D tailwindcss postcss autoprefixer

# 安装shadcn/ui依赖
npm install class-variance-authority clsx tailwind-merge tailwindcss-animate

# 安装Radix UI组件
npm install @radix-ui/react-dialog @radix-ui/react-label @radix-ui/react-select @radix-ui/react-slot @radix-ui/react-tabs

# 安装shadcn组件
npx shadcn@latest add card --yes
npx shadcn@latest add button --yes
npx shadcn@latest add badge --yes
npx shadcn@latest add dialog --yes
npx shadcn@latest add input --yes
npx shadcn@latest add label --yes
npx shadcn@latest add select --yes
npx shadcn@latest add textarea --yes
npx shadcn@latest add tabs --yes
npx shadcn@latest add alert --yes

echo "安装完成！"
```

## 故障排查

### 问题1：TailwindCSS配置未找到

**错误信息**：
```
No Tailwind CSS configuration found
```

**解决方案**：
- 确认 `tailwind.config.js` 文件已创建
- 确认 `postcss.config.js` 文件已创建
- 重启开发服务器

### 问题2：组件导入错误

**错误信息**：
```
Cannot find module '@/components/ui/card'
```

**解决方案**：
1. 确认shadcn组件已安装
2. 检查 `src/components/ui/` 目录是否存在
3. 检查 `tsconfig.json` 中的路径别名配置
4. 重启开发服务器

### 问题3：CSS变量未定义

**错误信息**：
```
Unknown at rule @tailwind
```

**解决方案**：
- 这是编辑器警告，不影响运行
- 可以忽略，或安装TailwindCSS IntelliSense插件

### 问题4：npm安装失败

**解决方案**：
```powershell
# 清理缓存
npm cache clean --force

# 删除node_modules和package-lock.json
rm -r node_modules
rm package-lock.json

# 重新安装
npm install
```

## 验证清单

安装完成后，检查以下内容：

- [ ] `tailwind.config.js` 文件存在
- [ ] `postcss.config.js` 文件存在
- [ ] `components.json` 文件存在
- [ ] `src/lib/utils.ts` 文件存在
- [ ] `src/components/ui/` 目录存在
- [ ] `src/components/ui/` 目录下有10个组件文件
- [ ] `package.json` 中包含TailwindCSS依赖
- [ ] `package.json` 中包含Radix UI依赖
- [ ] 前端服务可以正常启动
- [ ] 访问 `/config/rdagent-llm` 页面无错误

## 预期的package.json依赖

安装完成后，`package.json` 应包含以下依赖：

```json
{
  "dependencies": {
    "@radix-ui/react-dialog": "^1.x.x",
    "@radix-ui/react-label": "^2.x.x",
    "@radix-ui/react-select": "^2.x.x",
    "@radix-ui/react-slot": "^1.x.x",
    "@radix-ui/react-tabs": "^1.x.x",
    "class-variance-authority": "^0.x.x",
    "clsx": "^2.x.x",
    "lucide-react": "^0.563.0",
    "tailwind-merge": "^2.x.x",
    "tailwindcss-animate": "^1.x.x"
  },
  "devDependencies": {
    "autoprefixer": "^10.x.x",
    "postcss": "^8.x.x",
    "tailwindcss": "^3.x.x"
  }
}
```

## 下一步

安装完成后：

1. 启动后端服务（如果未启动）
2. 启动前端服务
3. 访问 `http://localhost:3000/config/rdagent-llm`
4. 在左侧导航栏"RD-Agent管理"下找到"🤖 RDagent 模型配置"
5. 测试页面功能

## 相关文档

- TailwindCSS官方文档：https://tailwindcss.com/docs
- Shadcn/UI官方文档：https://ui.shadcn.com
- Radix UI官方文档：https://www.radix-ui.com

---

**创建时间**：2026-02-05  
**状态**：配置文件已创建，等待执行安装命令
