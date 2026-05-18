# 项目文件结构

## 目录概览

```
stock/
├── config/                     # 配置
├── data_engine/                # 数据引擎（获取+处理）
│   ├── api/                    # API 封装
│   ├── core/                   # 核心数据结构 【新增】
│   ├── fetchers/               # 数据获取
│   ├── helpers/                # 辅助函数
│   └── processors/             # 数据处理
├── factor_engine/              # 因子引擎（分析+回测）
│   └── backtest/               # 回测模块
├── examples/                   # 示例代码 【新增】
├── docs/                       # 文档 【新增/更新】
├── scripts/                    # 脚本入口
│   ├── data_fetching/          # 数据获取脚本
│   ├── matrix_building/        # 矩阵构建脚本
│   └── factor_analysis/        # 因子分析脚本
├── utils/                      # 工具函数
└── tests/                      # 测试
```

---

## 核心模块

### 1. data_engine/core/ 【新增】

矩阵数据结构封装。

| 文件 | 说明 | 状态 |
|-----|------|------|
| `__init__.py` | 导出 FactorMatrix | 新增 |
| `factor_matrix.py` | FactorMatrix 类（内部 DataFrame） | 新增 |

### 2. data_engine/fetchers/

数据获取模块。

| 文件 | 说明 | 状态 |
|-----|------|------|
| `base_fetcher.py` | 获取器基类 | 原有 |
| `basic_fetcher.py` | 基础信息获取 | 原有 |
| `daily_fetcher.py` | 日线数据获取 | 原有 |
| `daily_basic_fetcher.py` | 每日指标获取 | 原有 |
| `st_fetcher.py` | ST 数据获取 | 原有 |
| `income_fetcher.py` | 利润表数据获取 | 原有 |
| `index_data_fetcher.py` | 指数数据获取 | 原有 |
| `limit_fetcher.py` | 涨跌停数据获取 | 原有 |
| `suspension_fetcher.py` | 停牌数据获取 | 原有 |
| `risk_free_rate_fetcher.py` | 无风险利率获取 | 原有 |
| `industry_fetcher.py` | 行业数据获取 | 原有 |

### 3. data_engine/processors/

数据处理模块。

| 文件 | 说明 | 状态 |
|-----|------|------|
| `matrix_io.py` | 矩阵读写工具 | 原有 |
| `matrix_builder.py` | 矩阵构建原子操作 | 原有 |
| `matrix_processor.py` | 矩阵业务编排 | 原有 |
| `data_loader.py` | 数据加载工具函数 | **更新** |
| `financial_matrix_builder.py` | 财务因子矩阵构建 | 原有 |
| `industry_matrix_builder.py` | 行业矩阵构建 | 原有 |
| `factor_matrix_builder.py` | 因子矩阵构建基类 | 原有 |
| `parallel_utils.py` | 并行处理工具 | 原有 |

### 4. factor_engine/backtest/

回测模块。

| 文件 | 说明 | 状态 |
|-----|------|------|
| `backtest_engine.py` | **新的回测引擎**（含可视化） | **新增/主要** |
| `single_factor_analyzer.py` | 原单因子分析器 | 保留 |
| `factor_analysis_runner.py` | 原分析运行器 | 保留 |
| `factor_analyzer.py` | 原分析器框架 | 保留 |
| `grouping.py` | 分组逻辑 | 保留 |
| `weighting.py` | 加权计算 | 保留 |
| `metrics.py` | 风险指标 | 保留 |
| `visualization.py` | 可视化 | 保留 |
| `transaction_cost.py` | 交易成本 | 保留 |
| `export.py` | 结果导出 | 保留 |
| `data_loader.py` | 回测数据加载 | 保留 |

---

## 示例和文档

### examples/ 【新增】

| 文件 | 说明 |
|-----|------|
| `quick_start.py` | 快速开始示例（最简单用法） |
| `backtest_engine_demo.py` | 完整功能演示 |
| `factor_matrix_demo.py` | FactorMatrix 用法演示 |

### docs/ 【新增/更新】

| 文件 | 说明 |
|-----|------|
| `factor_matrix_and_backtest.md` | 完整使用文档 |
| `project_structure.md` | 本文档 |

---

## 脚本入口

### scripts/data_fetching/

数据获取脚本。

| 文件 | 说明 |
|-----|------|
| `fetch_basic_data.py` | 获取基础信息 |
| `fetch_daily_data.py` | 获取日线数据 |
| `fetch_st_status.py` | 获取 ST 状态 |
| `fetch_income.py` | 获取利润表 |
| `...` | 其他数据获取脚本 |

### scripts/matrix_building/

矩阵构建脚本。

| 文件 | 说明 |
|-----|------|
| `build_valuation_matrices.py` | 估值因子矩阵 |
| `build_return_matrix.py` | 收益矩阵 |
| `build_liquidity_matrices.py` | 流动性因子矩阵 |
| `build_momentum_matrices.py` | 动量因子矩阵 |
| `build_income_matrices.py` | 财务因子矩阵 |
| `merge_factors_to_long.py` | 合并因子为长格式 |

### scripts/factor_analysis/

因子分析脚本。

| 文件 | 说明 |
|-----|------|
| `analyze_pb_factor.py` | PB 因子分析 |
| `analyze_net_profit_yoy_factor.py` | 净利润 YoY 分析 |
| `...` | 其他因子分析脚本 |

---

## 关键变更说明

### 新增文件

1. **`data_engine/core/factor_matrix.py`** - 矩阵数据结构封装
   - 内部使用 pandas DataFrame
   - 提供 rank/zscore/align 等方法

2. **`factor_engine/backtest/backtest_engine.py`** - 新回测引擎
   - 基于 FactorMatrix
   - 内置可视化方法
   - 支持一键生成报告

3. **`examples/`** - 示例代码
   - 帮助快速上手新 API

### 保留文件

原有回测系统（`single_factor_analyzer.py` 等）完全保留，可继续使用。

### 推荐用法

- **新项目/新回测**：使用 `BacktestEngine` + `FactorMatrix`
- **已有代码**：可继续使用 `SingleFactorAnalyzer`，或逐步迁移

---

## 数据存储位置

数据存储在项目外独立目录（`~/Documents/stockdata/`）：

```
stockdata/
├── basic/                      # 基础信息
├── daily/                      # 日线数据（每只股票一个 CSV）
├── factor/                     # 因子矩阵
├── matrices/                   # 基础矩阵（ST、可交易性、收益等）
├── supplementary/              # 辅助数据（市值、日历等）
└── backtest_results/           # 回测结果 【新增输出目录】
```
