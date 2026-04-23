# 股票数据获取与选股系统

一个集股票数据获取、技术指标计算和智能选股于一体的自动化系统。

## 功能特性

- **数据获取**: 从Tushare获取最近150个交易日的股票数据
- **增量更新**: 支持全量新增和增量补充
- **技术指标计算**: 自动计算89日移动平均线、量比等技术指标
- **智能选股**: 午盘选股策略（放量上涨+89日线穿越）
- **定时任务**: 支持定时自动更新数据和选股

## 选股策略

系统采用以下选股条件：

1. **放量上涨**: 量比 > 1.5（成交量放大50%以上）
2. **89日线穿越**: 最低价 < 89日线，且11:30收盘价 > 89日线
3. **涨幅限制**: 涨幅 < 8%（避免追高风险）

## 项目结构

```
stock_selection_system/
├── config/              # 配置文件
│   ├── settings.py      # 系统配置
│   └── constants.py     # 常量定义
├── core/                # 核心模块
│   ├── database.py      # 数据库操作
│   ├── data_fetcher.py  # 数据获取
│   ├── technical_analysis.py  # 技术指标计算
│   └── stock_selector.py      # 选股策略
├── clients/             # 数据源客户端
│   └── tushare_client.py
├── models/              # 数据模型
├── utils/               # 工具模块
├── main.py              # 主程序入口
└── requirements.txt     # 依赖包
```

## 安装

1. 安装依赖包：
```bash
pip install -r requirements.txt
```

2. 配置Tushare Token：
在 `config/settings.py` 中配置您的Tushare Token：
```python
TUSHARE_TOKEN = 'your_token_here'
```

## 使用方法

### 1. 初始化系统（首次运行）
```bash
python main.py --mode init
```
这将更新股票列表并获取所有历史数据。

### 2. 增量更新数据
```bash
python main.py --mode update
```
只更新缺失的数据。

### 3. 执行选股
```bash
python main.py --mode select
```
执行午盘选股策略。

### 4. 启动定时任务
```bash
python main.py --mode schedule
```
启动定时任务调度器，自动执行数据更新和选股。

### 5. 测试系统
```bash
python main.py --mode test
```
测试数据库连接和Tushare连接。

## 定时任务配置

系统默认配置以下定时任务：

- **16:00**: 更新当日股票数据
- **16:30**: 计算技术指标
- **11:30**: 执行午盘选股策略

可在 `config/settings.py` 中修改定时任务配置。

## 数据库说明

系统使用SQLite数据库，包含以下数据表：

1. **stock_info**: 股票基本信息
2. **daily_data**: 日线数据（包含技术指标）
3. **technical_indicators**: 技术指标详情
4. **stock_selection**: 选股结果记录

## 选股结果

选股结果将保存到数据库，并导出为CSV文件：
- 文件名格式: `selection_results_YYYYMMDD.csv`
- 包含股票代码、名称、涨幅、量比、89日线等详细信息

## 注意事项

1. 确保Tushare Token有效且有足够的权限
2. 首次运行建议使用 `--mode init` 进行初始化
3. 选股时间为11:30，需要确保数据已更新到当日
4. 建议在收盘后（16:00后）更新数据

## 技术栈

- Python 3.8+
- SQLite
- Tushare
- Pandas
- Schedule

## 许可证

MIT License
