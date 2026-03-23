# A-Share Quantitative Data Acquisition

基于 Tushare Pro API 的 A 股量化数据抓取系统。

## 功能特点

- ✅ 抓取所有 A 股上市公司基础信息（包括退市公司）
- ✅ 抓取股票日线行情数据（后复权）
- ✅ 支持断点续传
- ✅ 完善的日志记录
- ✅ 模块化设计

## 环境要求

- Python 3.10
- Conda（推荐）
- Tushare Pro API Token

## 快速开始

### 1. 创建 Conda 环境

```bash
conda env create -f environment.yml
conda activate stock
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env`：

```bash
cp .env.example .env
```

编辑 `.env` 文件，填入你的 Tushare Token 和 API URL：

```
TUSHARE_TOKEN=your_actual_token
TUSHARE_API_URL=your_actual_api_url
```

**注意**：`.env` 文件不会被提交到 git，请妥善保管。

### 3. 抓取基础数据

```bash
python scripts/fetch_basic_data.py
```

输出文件：`data/basic/all_companies_info.csv`

### 4. 抓取日线数据

```bash
# 测试单只股票
python scripts/fetch_daily_data.py --start-index 0 --batch-size 1

# 全量抓取（约 5800 只股票，需要 30 分钟）
python scripts/fetch_daily_data.py
```

输出目录：`data/daily/`

## 项目结构

```
stock/
├── config/                   # 配置模块
│   └── config.py
├── src/                      # 核心源码
│   ├── tushare_client.py     # Tushare 客户端封装
│   ├── data_fetcher.py       # 数据抓取逻辑
│   └── utils.py              # 工具函数
├── scripts/                  # 执行脚本
│   ├── fetch_basic_data.py   # 基础数据抓取
│   └── fetch_daily_data.py   # 日线数据抓取
└── data/                     # 数据目录
    ├── basic/                # 基础数据
    ├── daily/                # 日线行情
    └── logs/                 # 日志文件
```

## 数据说明

### 基础数据字段

- 股票信息：ts_code, name, industry, market
- 上市信息：list_date, delist_date, list_status
- 公司信息：chairman, manager, reg_capital
- IPO 信息：ipo_date, issue_date, price, funds

### 日线数据字段

- 日期：trade_date
- 价格：open, high, low, close（后复权）
- 成交：vol, amount

## 注意事项

- 需要 Tushare Pro API 权限（至少 120 积分）
- 日线数据抓取时间较长，建议使用断点续传
- 数据文件约占用 10 GB 磁盘空间

## License

MIT License
