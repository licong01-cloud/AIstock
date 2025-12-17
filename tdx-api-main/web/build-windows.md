# Windows EXE 编译指南

## ✅ 可以直接编译成EXE文件

TDX程序使用纯Go语言编写，**可以直接编译成Windows EXE文件**。

---

## 📋 编译要求

### 系统要求
- **操作系统**: Windows 7/8/10/11 (64位)
- **Go版本**: Go 1.20 或更高版本
- **网络**: 编译时需要下载依赖（首次编译）

### 检查Go环境
```bash
go version
```
应该显示类似：`go version go1.20.x windows/amd64`

---

## 🚀 快速编译

### 方法1: 使用编译脚本（推荐）

1. **进入web目录**
   ```bash
   cd tdx-api-main\tdx-api-main\web
   ```

2. **运行编译脚本**
   ```bash
   ..\build-windows.bat
   ```
   或者直接双击 `build-windows.bat`

3. **编译完成**
   - 生成文件：`web\stock-api.exe`
   - 文件大小：约 15-25 MB（已优化）

### 方法2: 手动编译

1. **进入项目根目录**
   ```bash
   cd tdx-api-main\tdx-api-main
   ```

2. **下载依赖**
   ```bash
   go mod download
   ```

3. **编译Web服务器**
   ```bash
   cd web
   go build -ldflags="-s -w" -o stock-api.exe .
   ```

### 方法3: 一键编译（最简单）

```bash
cd tdx-api-main\tdx-api-main\web
go build -ldflags="-s -w" -o stock-api.exe .
```

---

## 📦 编译参数说明

### 基础编译
```bash
go build -o stock-api.exe .
```

### 优化编译（推荐）
```bash
go build -ldflags="-s -w" -o stock-api.exe .
```
- `-s`: 去除符号表
- `-w`: 去除调试信息
- 结果：文件更小，运行更快

### 静态编译（完全独立）
```bash
set CGO_ENABLED=0
go build -ldflags="-s -w" -o stock-api.exe .
```
- `CGO_ENABLED=0`: 禁用CGO，纯Go编译
- 结果：不依赖任何外部库，完全独立运行

---

## 📁 文件结构要求

编译后的EXE文件需要以下目录结构：

```
stock-api.exe          # 编译生成的exe文件
static/                # 静态文件目录（必需）
  ├── index.html
  ├── style.css
  └── app.js
```

**重要**: `static` 目录必须与 `stock-api.exe` 在同一目录下！

---

## 🎯 运行EXE文件

### 方式1: 双击运行
直接双击 `stock-api.exe`，会启动Web服务器

### 方式2: 命令行运行
```bash
cd web
stock-api.exe
```

### 方式3: 后台运行（不显示窗口）
```bash
start /b stock-api.exe
```

### 访问应用
打开浏览器访问：`http://localhost:8080`

---

## 🔧 编译选项

### 1. 指定输出文件名
```bash
go build -o tdx-stock-api.exe .
```

### 2. 添加版本信息
```bash
go build -ldflags="-s -w -X main.Version=1.0.0" -o stock-api.exe .
```

### 3. 交叉编译（从Linux/Mac编译Windows版本）
```bash
GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o stock-api.exe .
```

### 4. 32位版本
```bash
GOARCH=386 go build -ldflags="-s -w" -o stock-api-32.exe .
```

---

## ⚠️ 常见问题

### Q1: 编译失败，提示找不到模块
**解决方案**:
```bash
cd tdx-api-main\tdx-api-main
go mod download
go mod tidy
```

### Q2: 编译失败，提示CGO错误
**解决方案**:
```bash
set CGO_ENABLED=0
go build -ldflags="-s -w" -o stock-api.exe .
```

### Q3: EXE文件无法运行，提示缺少DLL
**解决方案**:
- 使用静态编译：`set CGO_ENABLED=0`
- 或安装Visual C++ Redistributable

### Q4: 运行EXE后无法访问网页
**检查项**:
1. 防火墙是否阻止了8080端口
2. 是否有其他程序占用8080端口
3. 查看EXE运行窗口的错误信息

### Q5: 静态文件无法加载
**解决方案**:
- 确保 `static` 目录与 `stock-api.exe` 在同一目录
- 检查 `static` 目录下是否有 `index.html`、`style.css`、`app.js`

---

## 📊 编译结果

### 文件大小对比

| 编译方式 | 文件大小 | 说明 |
|---------|---------|------|
| 普通编译 | ~25-30 MB | 包含调试信息 |
| 优化编译 (`-s -w`) | ~15-20 MB | 去除符号表和调试信息 |
| 静态编译 (`CGO_ENABLED=0`) | ~15-20 MB | 完全独立，无外部依赖 |

### 性能对比

| 编译方式 | 启动速度 | 运行性能 |
|---------|---------|---------|
| 普通编译 | 正常 | 正常 |
| 优化编译 | 稍快 | 相同 |
| 静态编译 | 稍快 | 相同 |

---

## 🎁 打包发布

### 创建发布包

1. **创建发布目录**
   ```bash
   mkdir release
   ```

2. **复制文件**
   ```bash
   copy stock-api.exe release\
   xcopy /E /I static release\static
   ```

3. **创建启动脚本** (`release\start.bat`)
   ```batch
   @echo off
   start stock-api.exe
   echo 服务器已启动，访问 http://localhost:8080
   pause
   ```

4. **创建README** (`release\README.txt`)
   ```
   TDX股票数据API
   
   使用方法:
   1. 双击 start.bat 启动服务器
   2. 打开浏览器访问 http://localhost:8080
   
   注意:
   - 需要网络连接访问通达信服务器
   - 默认端口8080，可在代码中修改
   ```

---

## 🔍 验证编译结果

### 检查EXE文件
```bash
# 查看文件信息
dir stock-api.exe

# 测试运行（5秒后自动停止）
timeout /t 5 /nobreak >nul & stock-api.exe
```

### 功能测试
1. 运行 `stock-api.exe`
2. 访问 `http://localhost:8080`
3. 测试搜索股票功能
4. 测试K线图显示
5. 测试分时图显示

---

## 📝 编译脚本示例

### 完整编译脚本 (`build.bat`)
```batch
@echo off
chcp 65001 >nul
echo ========================================
echo   TDX股票数据API - 编译脚本
echo ========================================
echo.

cd /d %~dp0
cd ..

echo [1/3] 下载依赖...
go mod download
if %errorlevel% neq 0 (
    echo 错误: 依赖下载失败
    pause
    exit /b 1
)

echo.
echo [2/3] 编译程序...
cd web
set CGO_ENABLED=0
go build -ldflags="-s -w" -o stock-api.exe .
if %errorlevel% neq 0 (
    echo 错误: 编译失败
    pause
    exit /b 1
)

echo.
echo [3/3] 验证文件...
if exist stock-api.exe (
    echo ✓ 编译成功！
    echo.
    echo 文件位置: %cd%\stock-api.exe
    echo 文件大小: 
    dir stock-api.exe | find "stock-api.exe"
) else (
    echo ✗ 编译失败：未找到exe文件
)

echo.
pause
```

---

## ✅ 总结

**TDX程序可以直接编译成EXE文件**，具有以下特点：

1. ✅ **纯Go实现**：无需CGO，可以静态编译
2. ✅ **单文件运行**：编译后只需一个EXE文件（加上static目录）
3. ✅ **跨平台支持**：可以在Windows/Linux/Mac上编译
4. ✅ **无外部依赖**：编译后的EXE可以独立运行
5. ✅ **体积适中**：优化后约15-20MB

**推荐编译命令**:
```bash
cd web
set CGO_ENABLED=0
go build -ldflags="-s -w" -o stock-api.exe .
```

编译完成后，将 `static` 目录与 `stock-api.exe` 放在一起即可运行！

