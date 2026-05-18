# 项目文件结构（最终版）

## 目录概览

```
stock/
├── config/                     # 配置
├── data_engine/                # 数据引擎
│   ├── api/                    # API 封装（tushare）
│   ├── core/                   # 核心数据结构 【新增】
│   ├── fetchers/               # 数据获取
│   ├── helpers/                # 辅助函数
│   └── processors/             # 数据处理
├── factor_engine/              # 因子引擎（简化后）
│   └── backtest/               # 回测模块（新引擎）
├── examples/                   # 示例代码 【新增】
├── docs/                       # 文档 【新增】
├── scripts/                    # 脚本入口
│   ├── data_fetching/          # 数据获取脚本
│   ├── matrix_building/        # 矩阵构建脚本
│   └── factor_analysis/        # 因子分析脚本（已简化）
├── utils/                      # 工具函数
└── tests/                      # 测试
```

---

## 核心模块

### 1. data_engine/core/ 【新增】

矩阵数据结构封装。

| 文件 | 说明 |
|-----|------|
| `__init__.py` | 导出 FactorMatrix |
| `factor_matrix.py` | FactorMatrix 类（内部 DataFrame） |

### 2. factor_engine/backtest/ 【简化后】

**已删除的文件：**
- ❌ `single_factor_analyzer.py`
- ❌ `factor_analysis_runner.py`
- ❌ `factor_analyzer.py`
- ❌ `grouping.py`
- ❌ `weighting.py`
- ❌ `metrics.py`
- ❌ `visualization.py`
- ❌ `transaction_cost.py`
- ❌ `export.py`
- ❌ `data_loader.py`

**保留的文件：**
| 文件 | 说明 |
|-----|------|
| `__init__.py` | 导出 BacktestEngine 等 |
| `backtest_engine.py` | **新的回测引擎（含所有功能）** |

### 3. scripts/factor_analysis/ 【简化后】

**已删除的冗余脚本：**
- ❌ `analyze_all_income_factors.py`
- ❌ `analyze_ebitda_yoy_factor.py`
- ❌ `analyze_factor_unified.py`
- ❌ `analyze_liquidity_all_stocks.py`
- ❌ `analyze_liquidity_in_microcap.py`
- ❌ `analyze_momentum_in_microcap.py`
- ❌ `analyze_pb_factor_with_cost.py`
- ❌ `analyze_revenue_yoy_factor.py`
- ❌ `combine_factors.py`
- ❌ `diagnose_factor_data.py`
- ❌ `test_transaction_cost.py`

**保留并重写的脚本：**
| 文件 | 说明 |
|-----|------|
| `analyze_pb_factor.py` | PB 因子分析 |
| `analyze_net_profit_yoy_factor.py` | 净利润 YoY 分析 |
| `analyze_mv_factor.py` | 市值因子分析 |
| `analyze_factors_in_microcap.py` | 微盘股内因子分析 |
| `_template.py` | 新脚本模板 |

---

## 关键变更

### 被删除的模块

所有旧的回测相关模块已删除，功能整合到 `backtest_engine.py`：

| 旧模块 | 功能整合到 |
|-------|-----------|
| `single_factor_analyzer.py` | `BacktestEngine.run()` |
| `grouping.py` | `BacktestEngine._group_factor()` |
| `weighting.py` | `BacktestEngine._calculate_group_returns()` |
| `metrics.py` | `BacktestEngine._calculate_statistics()` |
| `visualization.py` | `BacktestResult.plot_*()` |
| `transaction_cost.py` | `BacktestConfig` 参数 |
| `export.py` | `BacktestResult.save()` |

### 新的使用方式

**原来（已删除）：**
```python
from factor_engine import SingleFactorAnalyzer
from factor_engine.backtest.visualization import plot_combined_returns

analyzer = SingleFactorAnalyzer(...)
results = analyzer.run_analysis(...)
plot_combined_returns(...)
```

**现在：**
```python
from factor_engine.backtest import BacktestEngine, BacktestConfig

engine = BacktestEngine(config=BacktestConfig(...))
engine.load_factor(factor)
...
result = engine.run()
result.generate_report(output_dir)  # 一键生成数据+图表
```

---

## 文件数量对比

| 类型 | 原数量 | 现数量 | 变化 |
|-----|-------|-------|------|
| backtest 模块 | 11 个 | 2 个 | **-9** |
| factor_analysis 脚本 | 14 个 | 5 个 | **-9** |
| 总计 | 25 个 | 7 个 | **-18** |

---

## 数据流

```
FactorMatrix (data_engine/core/)
    ↓
BacktestEngine (factor_engine/backtest/)
    ↓
BacktestResult (数据 + 图表)
```

---

## 快速开始

```python
# 加载因子矩阵
from data_engine.core.factor_matrix import FactorMatrix
factor = FactorMatrix.from_csv('pb_matrix.csv', name='pb')

# 运行回测
from factor_engine.backtest import BacktestEngine, BacktestConfig
engine = BacktestEngine(config=BacktestConfig(n_groups=10))
engine.load_factor(factor)
...
result = engine.run()

# 生成报告
result.generate_report(output_dir)
```
