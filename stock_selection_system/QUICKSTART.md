# 快速开始指南

## 第一步：安装依赖

```bash
cd stock_selection_system
pip install -r requirements.txt
```

## 第二步：配置Tushare Token

在 `config/settings.py` 中已经配置了Token，如需修改：

```python
TUSHARE_TOKEN = 'your_token_here'
```

## 第三步：初始化系统

首次运行需要初始化数据库和获取历史数据：

```bash
python main.py --mode init
```

这个过程可能需要较长时间，因为要获取所有A股的150个交易日数据。

## 第四步：执行选股

初始化完成后，可以执行选股：

```bash
python main.py --mode select
```

## 日常使用

### 每日更新数据

```bash
python main.py --mode update
```

### 启动定时任务

```bash
python main.py --mode schedule
```

系统将自动在以下时间执行任务：
- 16:00: 更新数据
- 16:30: 计算技术指标
- 11:30: 执行选股

## 查看选股结果

选股结果保存在：
1. 数据库：`data/stock_data.db`
2. CSV文件：`selection_results_YYYYMMDD.csv`

## 常见问题

### 1. Tushare连接失败
- 检查Token是否正确
- 检查网络连接
- 确认Tushare账户权限

### 2. 数据获取失败
- 检查API调用次数限制
- 检查网络连接
- 查看日志文件：`logs/stock_selection.log`

### 3. 选股结果为空
- 确认数据已更新到当日
- 检查选股条件是否过于严格
- 查看日志了解详细情况

## 系统测试

运行测试模式检查系统状态：

```bash
python main.py --mode test
```

## 技术支持

如遇问题，请查看：
1. 日志文件：`logs/stock_selection.log`
2. README.md 文档
3. 规划文档：`DOC/规划.md`
