# 添加v25到execution_algorithm_catalog数据库

## 方法1: 通过SQL直接添加

连接到PostgreSQL数据库并执行以下SQL:



## 方法2: 通过backend API添加

如果backend服务正在运行，可以通过API添加:

<html>
<head>
 <title>500 Internal Privoxy Error</title>
 <link rel="shortcut icon" href="http://config.privoxy.org/error-favicon.ico" type="image/x-icon"></head>
<body>
<h1>500 Internal Privoxy Error</h1>
<p>Privoxy encountered an error while processing your request:</p>
<p><b>Could not load template file <code>forwarding-failed</code> or one of its included components.</b></p>
<p>Please contact your proxy administrator.</p>
<p>If you are the proxy administrator, please put the required file(s)in the <code><i>(confdir)</i>/templates</code> directory.  The location of the <code><i>(confdir)</i></code> directory is specified in the main Privoxy <code>config</code> file.  (It's typically the Privoxy install directory).</p>
</body>
</html>

## 方法3: 通过init_catalog_db.py添加

编辑 ，在V24_PLAN的INSERT语句后添加v25的记录，然后运行:



## 验证

执行以下SQL验证v25已添加:



预期输出应包含:


## 前端验证

1. 重启backend服务（如果正在运行）
2. 打开前端页面
3. 进入任意QE实验配置页面
4. 在日内执行算法下拉框中应该能看到v25
