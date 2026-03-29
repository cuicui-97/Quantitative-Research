# 项目架构说明

## 双引擎设计

本项目采用**双引擎架构**，清晰地分离了数据处理和因子分析两大核心功能：

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层 (scripts/)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ 数据获取     │  │ 矩阵构建     │  │ 因子分析     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└──────────┬────────────────┬────────────────┬────────────────┘
           │                │                │
           ↓                ↓                ↓
┌─────────────────────┐            ┌─────────────────────┐
│   data_engine/      │            │   factor_engine/    │
│   ───────────────   │            │   ───────────────   │
│   数据引擎           │            │   因子引擎           │
│                     │            │                     │
│  ┌────────────┐    │            │  ┌────────────┐    │
│  │ API        │    │            │  │ Backtest   │    │
│  │ Fetchers   │    │            │  │ Factors    │    │
│  │ Processors │    │            │  │ Metrics    │    │
│  │ Utils      │    │            │  │ Visualize  │    │
│  └────────────┘    │            │  └────────────┘    │
└─────────────────────┘            └─────────────────────┘
           │                                │
           ↓                                ↓
┌─────────────────────────────────────────────────────────────┐
│                      数据存储层 (data/)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ basic/       │  │ matrices/    │  │ results/     │      │
│  │ daily/       │  │              │  │              │      │
│  │ supplementary│  │              │  │              │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

---

## 1. data_engine - 数据引擎

### 职责
负责**数据的获取、清洗、存储和矩阵构建**。

### 核心模块

```
data_engine/
├── api/                # API接口层
│   └── tushare_api.py  # Tushare API 封装
├── fetchers/           # 数据抓取器
│   ├── base_fetcher.py         # 抓取器基类
│   ├── basic_fetcher.py        # 基础数据抓取
│   ├── daily_fetcher.py        # 日线数据抓取
│   ├── st_fetcher.py           # ST状态抓取
│   ├── suspension_fetcher.py   # 停牌信息抓取
│   └── limit_fetcher.py        # 涨跌停价格抓取
├── processors/         # 数据处理器
│   ├── data_loader.py          # 数据加载
│   ├── matrix_builder.py       # 矩阵构建
│   ├── matrix_processor.py     # 矩阵处理
│   ├── matrix_io.py            # 矩阵读写
│   └── parallel_utils.py       # 并行处理工具
├── utils/              # 工具函数
│   ├── logger.py       # 日志工具
│   ├── retry.py        # 重试装饰器
│   ├── date.py         # 日期工具
│   ├── file.py         # 文件工具
│   └── data.py         # 数据工具
└── trade_calendar.py   # 交易日历
```

### 关键特性
- ✅ **增量更新**: 自动识别缺失数据，只获取增量部分
- ✅ **断点续传**: 长时间任务支持中断后继续
- ✅ **错误重试**: API失败自动重试（最多3次）
- ✅ **并行处理**: 矩阵构建支持多进程并行

### 使用示例

```python
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers import DailyDataFetcher
from data_engine.processors.matrix_io import load_matrix

# 获取数据
api = TushareAPI()
fetcher = DailyDataFetcher(api)
fetcher.fetch_all()

# 加载矩阵
pb_matrix = load_matrix('data/matrices/pb_matrix.csv')
```

---

## 2. factor_engine - 因子引擎

### 职责
负责**因子分析、回测、评估和可视化**。

### 核心模块

```
factor_engine/
├── backtest/                       # 回测框架
│   ├── single_factor_analyzer.py   # 单因子分析器（核心）
│   ├── grouping.py                 # 因子分组逻辑
│   ├── weighting.py                # 收益加权计算
│   ├── metrics.py                  # 指标计算
│   └── visualization.py            # 可视化
└── factors/                        # 因子库（预留）
    └── __init__.py
```

### 关键特性
- ✅ **模块化设计**: 分组、加权、指标、可视化各司其职
- ✅ **可复用框架**: 添加新因子只需传入因子矩阵
- ✅ **双重加权**: 支持等权和市值加权两种方式
- ✅ **完整指标**: 收益、波动、夏普、胜率、累计收益
- ✅ **自动可视化**: 累计收益曲线、统计柱状图

### 使用示例

```python
from factor_engine import SingleFactorAnalyzer
from data_engine.processors.matrix_io import load_matrix

# 加载数据
pb_matrix = load_matrix('data/matrices/pb_matrix.csv')
return_matrix = load_matrix('data/matrices/open_return_matrix.csv')
tradability_matrix = load_matrix('data/matrices/tradability_matrix.csv')
mv_matrix = load_matrix('data/matrices/circ_mv_matrix.csv')

# 创建分析器
analyzer = SingleFactorAnalyzer(
    factor_name='PB',
    factor_matrix=pb_matrix,
    return_matrix=return_matrix,
    tradability_matrix=tradability_matrix,
    mv_matrix=mv_matrix,
    n_groups=10
)

# 运行分析
results = analyzer.run_analysis(
    output_dir='factor_analysis_results/pb_factor',
    save_results=True
)
```

---

## 3. 应用层 (scripts/)

### 职责
提供**可执行的脚本**，组合使用 data_engine 和 factor_engine。

### 目录结构

```
scripts/
├── data_fetching/      # 数据获取脚本（使用 data_engine）
│   ├── fetch_basic_data.py
│   ├── fetch_daily_data.py
│   ├── fetch_st_status.py
│   └── ...
├── matrix_building/    # 矩阵构建脚本（使用 data_engine）
│   ├── build_all_matrices.py
│   ├── build_price_matrices.py
│   └── ...
└── factor_analysis/    # 因子分析脚本（使用 factor_engine）
    ├── analyze_pb_factor_v2.py
    ├── analyze_mv_factor_v2.py
    └── ...
```

---

## 设计优势

### 1. **职责清晰**
- `data_engine`: 只负责数据，不关心因子分析逻辑
- `factor_engine`: 只负责因子分析，不关心数据来源

### 2. **解耦合**
- 两个引擎可以独立开发、测试、发布
- `factor_engine` 可以单独作为因子分析库使用
- 未来可以轻松添加 `strategy_engine`（策略引擎）、`risk_engine`（风控引擎）

### 3. **易扩展**
- 添加新的数据源：只需扩展 `data_engine/fetchers/`
- 添加新的因子：只需使用 `factor_engine` 框架
- 添加新的策略：未来可以创建 `strategy_engine`

### 4. **可复用**
- `data_engine` 可以用于其他量化项目
- `factor_engine` 可以用于任何因子分析任务

---

## 数据流转

```
1. 数据获取阶段
   Tushare API → data_engine/fetchers → data/basic/
                                      → data/daily/
                                      → data/supplementary/

2. 矩阵构建阶段
   data/basic/ + data/daily/ → data_engine/processors → data/matrices/

3. 因子分析阶段
   data/matrices/ → factor_engine/backtest → factor_analysis_results/
```

---

## 类比其他项目

这种双引擎设计类似于：

| 项目 | 数据层 | 分析层 |
|------|--------|--------|
| **本项目** | `data_engine` | `factor_engine` |
| Backtrader | `backtrader.feeds` | `backtrader.strategies` |
| Zipline | `zipline.data` | `zipline.pipeline` |
| Rqalpha | `rqalpha.data` | `rqalpha.mod` |

---

## 未来扩展方向

```
stock/
├── data_engine/        # 数据引擎
├── factor_engine/      # 因子引擎
├── strategy_engine/    # 策略引擎（未来）
│   ├── portfolio/      # 组合构建
│   ├── rebalance/      # 调仓逻辑
│   └── execution/      # 执行模拟
├── risk_engine/        # 风控引擎（未来）
│   ├── exposure/       # 风险暴露
│   ├── attribution/    # 归因分析
│   └── stress/         # 压力测试
└── scripts/           # 应用脚本
```

---

## 总结

**双引擎架构的核心理念**：

> 数据是数据，分析是分析，各司其职，互不干扰。

这种设计让项目具有良好的**可维护性**、**可扩展性**和**可复用性**，是大型量化系统的标准架构模式。
