# Scripts 目录说明

本目录包含所有可执行脚本，按功能分类组织。

## 目录结构

```
scripts/
├── data_fetching/       # 数据获取相关脚本
├── matrix_building/     # 矩阵构建相关脚本
└── factor_analysis/     # 因子分析相关脚本
```

## 使用流程

### 1. 数据获取阶段（data_fetching/）

首次运行或需要更新数据时：

```bash
# 1. 检查API连接
python scripts/data_fetching/check_api_health.py

# 2. 获取基础数据（股票列表、交易日历）
python scripts/data_fetching/fetch_basic_data.py

# 3. 获取日线数据（价格、成交量等）
python scripts/data_fetching/fetch_daily_data.py

# 4. 获取补充数据
python scripts/data_fetching/fetch_st_status.py          # ST状态
python scripts/data_fetching/fetch_suspension_status.py  # 停牌状态
python scripts/data_fetching/fetch_limit_prices.py       # 涨跌停价格

# 5. 可选：获取Fama因子数据
python scripts/data_fetching/fetch_fama_data.py
python scripts/data_fetching/validate_fama_data.py
```

### 2. 矩阵构建阶段（matrix_building/）

数据获取完成后，构建分析所需的矩阵：

```bash
# 方式1：一键构建所有矩阵（推荐）
python scripts/matrix_building/build_all_matrices.py

# 方式2：按需构建
python scripts/matrix_building/build_trade_calendar.py   # 交易日历
python scripts/matrix_building/build_price_matrices.py   # 价格矩阵
python scripts/matrix_building/build_valuation_matrices.py  # 估值矩阵
```

### 3. 因子分析阶段（factor_analysis/）

矩阵构建完成后，进行因子回测分析：

```bash
# PB因子分析
python scripts/factor_analysis/analyze_pb_factor.py

# 市值因子分析
python scripts/factor_analysis/analyze_mv_factor.py

# 可交易性分析
python scripts/factor_analysis/analyze_tradability_matrix.py
```

## 详细说明

### data_fetching/ - 数据获取

| 脚本 | 功能 | 输出位置 |
|------|------|----------|
| `check_api_health.py` | 检查Tushare API连接状态 | 控制台输出 |
| `fetch_basic_data.py` | 获取股票基本信息 | `data/basic/stock_basic.csv` |
| `fetch_daily_data.py` | 获取股票日线数据 | `data/daily/{ts_code}.csv` |
| `fetch_st_status.py` | 获取ST状态数据 | `data/supplementary/st_status.csv` |
| `fetch_suspension_status.py` | 获取停牌状态数据 | `data/supplementary/suspension_status.csv` |
| `fetch_limit_prices.py` | 获取涨跌停价格数据 | `data/supplementary/limit_prices.csv` |
| `fetch_fama_data.py` | 获取Fama三因子数据 | `data/supplementary/fama_*.csv` |
| `validate_fama_data.py` | 验证Fama数据完整性 | 控制台输出 |

### matrix_building/ - 矩阵构建

| 脚本 | 功能 | 输出位置 |
|------|------|----------|
| `build_trade_calendar.py` | 构建交易日历矩阵 | `data/matrices/trade_calendar.csv` |
| `build_price_matrices.py` | 构建价格相关矩阵 | `data/matrices/open_matrix.csv`, `close_matrix.csv`, etc. |
| `build_valuation_matrices.py` | 构建估值矩阵 | `data/matrices/pb_matrix.csv`, `circ_mv_matrix.csv`, etc. |
| `build_all_matrices.py` | 一键构建所有矩阵 | `data/matrices/` |

### factor_analysis/ - 因子分析

| 脚本 | 功能 | 输出位置 |
|------|------|----------|
| `analyze_pb_factor.py` | PB因子回测分析 | `factor_analysis_results/pb_factor/` |
| `analyze_mv_factor.py` | 市值因子回测分析 | `factor_analysis_results/mv_factor/` |
| `analyze_tradability_matrix.py` | 可交易性分析 | 控制台输出 |

## 环境要求

所有脚本需要在 `stock` conda环境下运行：

```bash
conda activate stock
python scripts/<category>/<script_name>.py
```

## 注意事项

1. **数据依赖**: 矩阵构建依赖数据获取，因子分析依赖矩阵构建
2. **增量更新**: 大部分脚本支持增量更新，重复运行只会获取缺失数据
3. **断点续传**: 长时间任务支持断点续传，中断后重新运行会从断点继续
4. **日志输出**: 所有脚本都有详细的日志输出，便于追踪进度

## 开发指南

### 添加新的数据获取脚本

1. 在 `data_fetching/` 创建新脚本
2. 继承 `BaseFetcher` 基类
3. 使用 `@incremental_update` 装饰器实现增量更新
4. 更新本README的表格

### 添加新的因子分析

1. 复制 `factor_analysis/analyze_pb_factor.py` 作为模板
2. 修改因子矩阵加载逻辑
3. 输出到 `factor_analysis_results/<factor_name>/`
4. 更新本README的表格
