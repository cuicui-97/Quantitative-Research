# A-Share Quantitative Data Acquisition

基于 Tushare Pro API 的 A 股量化数据抓取系统。

## 功能特点

- ✅ 抓取所有 A 股上市公司基础信息（包括退市公司）
- ✅ 抓取股票日线行情数据（后复权/前复权/不复权）
- ✅ 构建交易可用性矩阵（Tradability Matrix）
- ✅ 构建开盘收益率矩阵（Open-to-Open Return Matrix）
- ✅ 支持断点续传
- ✅ 完善的日志记录
- ✅ 模块化设计

## 环境要求

- Python 3.10+
- Conda（推荐）
- Tushare Pro API Token（至少 120 积分）

## 快速开始

### 1. 安装环境

```bash
# 创建并激活 Conda 环境
conda env create -f environment.yml
conda activate stock
```

### 2. 配置 API

复制 `.env.example` 为 `.env`，填入你的 Tushare Token 和 API URL：

```bash
cp .env.example .env
# 编辑 .env 文件
```

### 3. 健康检查（推荐）

```bash
python scripts/check_api_health.py
```

### 4. 抓取数据

```bash
# 1. 基础数据
python scripts/fetch_basic_data.py

# 2. 日线数据（约 5800 只股票，需要 30 分钟）
python scripts/fetch_daily_data.py

# 3. 交易日历缓存
python scripts/build_trade_calendar.py

# 4. 补充数据
python scripts/fetch_st_status.py
python scripts/fetch_suspension_status.py
```

### 5. 构建矩阵

```bash
# 方式 1: 批量构建所有矩阵（推荐）
python scripts/build_all_matrices.py

# 方式 2: 只构建指定的矩阵（节省时间）
python scripts/build_all_matrices.py --matrices return              # 只构建开盘收益率矩阵
python scripts/build_all_matrices.py --matrices limit tradability   # 构建涨跌停和交易可用性矩阵
python scripts/build_all_matrices.py --min-listing-days 90          # 自定义最小上市天数

# 方式 3: 单独构建补充矩阵
python scripts/build_st_matrix.py
python scripts/build_suspension_matrix.py

# 查看所有选项
python scripts/build_all_matrices.py --help
```

**可用的矩阵类型**：
- `listing_days` - 上市天数矩阵
- `missing_data` - 数据缺失矩阵
- `limit` - 涨跌停矩阵
- `tradability` - 交易可用性矩阵
- `return` - 开盘收益率矩阵
- `all` - 所有矩阵（默认）

## 项目结构

```
stock/
├── config/                   # 配置
├── src/
│   ├── api/                  # API 层（直接调用 Tushare）
│   ├── fetchers/             # 数据获取层（业务逻辑）
│   ├── processors/           # 数据处理层（矩阵计算）
│   └── utils.py              # 工具函数
├── scripts/                  # 执行脚本
└── data/                     # 数据目录
    ├── basic/                # 基础数据
    ├── daily/                # 日线数据
    ├── supplementary/        # 补充数据（ST、停牌、交易日历）
    └── matrices/             # 矩阵数据
```

## 输出数据

### 基础数据
- `data/basic/all_companies_info.csv` - 所有上市公司信息

### 日线数据
- `data/daily/{ts_code}.csv` - 每只股票的日线数据（包含三种复权价格）

### 补充数据
- `data/supplementary/trade_calendar.csv` - 交易日历
- `data/supplementary/st_status.csv` - ST 状态数据（2016-01-01 起）
- `data/supplementary/st_matrix.csv` - ST 状态矩阵（1=ST，0=非ST）
- `data/supplementary/suspension_status.csv` - 停牌信息
- `data/supplementary/suspension_matrix.csv` - 停牌状态矩阵（1=停牌，0=正常）

### 矩阵数据
- `data/matrices/listing_days_matrix.csv` - 上市天数矩阵（1=满足180天，0=不满足）
- `data/matrices/missing_data_matrix.csv` - 数据缺失矩阵（1=有数据，0=缺失）
- `data/matrices/limit_matrix.csv` - 涨跌停矩阵（1=未涨跌停，0=涨跌停）
- `data/matrices/tradability_matrix.csv` - 交易可用性矩阵（1=可交易，0=不可交易）
- `data/matrices/open_return_matrix.csv` - 开盘收益率矩阵（用于回测，NaN=无数据）

## 注意事项

- ST 状态数据从 2016-01-01 开始（API 限制）
- 日线数据抓取时间较长，支持断点续传
- 数据文件约占用 10 GB 磁盘空间
- API 调用间隔 0.3 秒（自动控制）

## 文档

- `README.md` - 本文档（使用指南）
- `PROJECT.md` - 项目架构文档（面向开发者）

## License

MIT License
