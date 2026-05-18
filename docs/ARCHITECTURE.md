# 系统架构文档

> 本文档记录系统的架构设计、关键决策和实现细节

## 一、双引擎架构

```
┌─────────────────────────────────────────────────────────┐
│                        脚本层                            │
│         (scripts/ - 简单调用，无复杂逻辑)                 │
├────────────────────┬────────────────────────────────────┤
│    data_engine     │          factor_engine             │
│    (数据引擎)       │           (因子引擎)                │
├────────────────────┼────────────────────────────────────┤
│  fetchers/         │  backtest/                         │
│  - 数据获取        │  - grouping.py      分组           │
│  - 统一继承BaseFetcher                              │
│                    │  - weighting.py     加权           │
│  processors/       │  - metrics.py       风险指标       │
│  - 矩阵构建        │  - visualization.py 可视化         │
│  - 矩阵处理        │  - export.py        导出           │
│                    │  - factor_analysis_runner.py       │
│  utils/            │                                    │
│  - 交易日历        │                                    │
│  - ST工具          │                                    │
└────────────────────┴────────────────────────────────────┘
```

## 二、关键架构决策

### 2.1 数据存储位置

```python
# config/config.py
DATA_DIR = Path('/Users/cuicui/Documents/stockdata')  # 独立于项目目录
```

**原因**：数据量大（~10GB），独立存储便于管理

### 2.2 脚本路径配置

```python
# 所有 scripts/ 下的脚本统一使用
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
```

**历史问题**：曾有64%的脚本使用错误的 `parent.parent.parent`，已全部修复

### 2.3 Fetcher继承体系

**所有Fetcher必须继承BaseFetcher**：

```python
class XxxFetcher(BaseFetcher):
    def __init__(self, api: TushareAPI):
        super().__init__(api, use_output_dir=True/False)
```

**历史问题**：曾有3个Fetcher未继承，已修复

### 2.4 矩阵构建框架

**重构后使用基类模式**：

```python
from data_engine.processors import FactorMatrixBuilder

class MyFactorBuilder(FactorMatrixBuilder):
    def get_factor_definitions(self):
        return [('factor_name', '因子中文名')]

    def calculate_factors_for_stock(self, ts_code, close, **kwargs):
        return {'factor_name': close.pct_change(20)}
```

**基类提供**：
- `init_matrix()` - 初始化NaN矩阵
- `load_stock_data()` - 加载股票数据
- `process_all_stocks()` - 逐股票处理
- `save_results()` - 保存和统计

### 2.5 因子分析框架

**统一接口**：

```python
from factor_engine.backtest import run_single_factor_analysis

run_single_factor_analysis(
    factor_name='MyFactor',
    factor_matrix_file='my_factor_matrix.csv',
    stock_pool='all',  # all/microcap/zz1000
    enable_cost=True
)
```

## 三、数据流

### 3.1 完整数据流程

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   原始数据    │    │   补充数据    │    │   矩阵数据    │
├──────────────┤    ├──────────────┤    ├──────────────┤
│ basic/       │    │ st_status.csv│    │ *_matrix.csv │
│ daily/       │    │ suspension_  │    │              │
│              │    │ limit_prices │    │              │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                    ┌──────▼──────┐
                    │  因子分析    │
                    │ factor_     │
                    │ analysis    │
                    └─────────────┘
```

### 3.2 ST数据融合策略

```
数据源：
├── stock_st API (2016年后) → st_status.csv (临时)
└── namechange历史 (全时段) → 提取ST → 融合

最终输出：st_status.csv (覆盖，包含完整历史)
备份：st_status_api_only.csv (仅首次生成)
```

**关键脚本**：
- `fetch_st_status.py` - 获取API数据
- `fetch_namechange_history.py` - 获取名称变更历史
- `fetch_and_merge_st_data.py` - 融合数据，**直接覆盖** st_status.csv

### 3.3 PIT数据处理

**真实PIT**：使用 `ann_date`（公告日）而非 `end_date`（报告期截止日）

```
报告期: 2024-03-31 (Q1)
公告日: 2024-04-25
使用区间: 2024-04-25 至下一次公告日前
```

## 四、模块职责

### 4.1 data_engine

| 子模块 | 职责 | 关键类/函数 |
|-------|------|------------|
| api/ | API封装 | TushareAPI |
| fetchers/ | 数据获取 | BaseFetcher, *Fetcher |
| processors/ | 矩阵处理 | FactorMatrixBuilder, MatrixBuilder, MatrixProcessor |
| utils/ | 工具函数 | trade_calendar, st_utils |

### 4.2 factor_engine

| 子模块 | 职责 | 关键类/函数 |
|-------|------|------------|
| backtest/ | 回测分析 | SingleFactorAnalyzer, run_single_factor_analysis |
| backtest/grouping.py | 因子分组 | FactorGrouper |
| backtest/weighting.py | 收益加权 | WeightCalculator |
| backtest/metrics.py | 风险指标 | PerformanceMetrics |
| backtest/visualization.py | 可视化 | plot_combined_returns |

## 五、目录结构速查

```
stock/
├── config/
│   └── config.py              # 配置（DATA_DIR路径等）
├── data_engine/
│   ├── api/
│   │   └── tushare_api.py     # API封装
│   ├── fetchers/
│   │   ├── base_fetcher.py    # 基类
│   │   ├── basic_fetcher.py
│   │   ├── daily_fetcher.py
│   │   └── ...                # 共10个Fetcher
│   ├── processors/
│   │   ├── factor_matrix_builder.py  # 因子构建基类 ⭐
│   │   ├── matrix_builder.py
│   │   ├── matrix_processor.py
│   │   ├── matrix_io.py
│   │   └── data_loader.py     # 含load_standard_matrices ⭐
│   └── utils/
│       ├── st_utils.py        # ST工具
│       ├── trade_calendar.py
│       └── logger.py
├── factor_engine/
│   └── backtest/
│       ├── factor_analysis_runner.py  # 统一接口 ⭐
│       ├── single_factor_analyzer.py
│       ├── grouping.py
│       ├── weighting.py
│       ├── metrics.py
│       └── visualization.py
├── scripts/
│   ├── data_fetching/         # 11个脚本
│   ├── matrix_building/       # 14个脚本
│   └── factor_analysis/       # 16个脚本
└── docs/                      # 本文档目录
```

## 六、设计原则

1. **职责单一**：每个模块只做一件事
2. **代码复用**：相同逻辑只定义一次（基类提取）
3. **向后兼容**：修改时保持接口兼容
4. **简单优先**：不做过度抽象
5. **矩阵优先**：核心计算在引擎层，脚本只负责调用

---

*详见 [REFACTORING.md](REFACTORING.md) 重构详情*
