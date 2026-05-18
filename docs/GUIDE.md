# 使用指南

> 详细的使用说明和API文档

## 目录

- [环境配置](#环境配置)
- [数据获取](#数据获取)
- [矩阵构建](#矩阵构建)
- [因子分析](#因子分析)
- [新增因子](#新增因子)
- [常见问题](#常见问题)

---

## 环境配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置Tushare Token

编辑 `.env` 文件：
```bash
TUSHARE_TOKEN=your_token_here
```

### 3. 数据目录

默认：`/Users/cuicui/Documents/stockdata/`

修改：`config/config.py` 中的 `DATA_DIR`

---

## 数据获取

### 完整数据流程（首次运行）

```bash
# 1. 股票基础信息
python scripts/data_fetching/fetch_basic_data.py

# 2. 日线数据
python scripts/data_fetching/fetch_daily_data.py --start-date 20150101

# 3. ST状态（三步完整流程）
python scripts/data_fetching/fetch_st_status.py
python scripts/data_fetching/fetch_namechange_history.py
python scripts/data_fetching/fetch_and_merge_st_data.py

# 4. 停牌和涨跌停
python scripts/data_fetching/fetch_suspension_status.py
python scripts/data_fetching/fetch_limit_prices.py

# 5. 每日指标
python scripts/data_fetching/fetch_daily_basic.py

# 6. 利润表（财务因子）
python scripts/data_fetching/fetch_income.py
```

---

## 矩阵构建

### 构建所有基础矩阵

```bash
python scripts/matrix_building/build_all_matrices.py
```

### 构建因子矩阵

```bash
# 动量/反转/流动性
python scripts/matrix_building/build_momentum_matrices.py
python scripts/matrix_building/build_liquidity_matrices.py

# 概念因子
python scripts/matrix_building/build_concept_factors.py

# 行业因子
python scripts/matrix_building/build_industry_factors.py

# 财务YoY
python scripts/matrix_building/build_income_matrices.py
```

---

## 因子分析

### 单因子分析（新接口）

```python
from factor_engine.backtest import run_single_factor_analysis

# 最简用法
run_single_factor_analysis(
    factor_name='NetProfitYoY',
    factor_matrix_file='n_income_attr_p_yoy_matrix.csv'
)

# 完整参数
run_single_factor_analysis(
    factor_name='NetProfitYoY',
    factor_matrix_file='n_income_attr_p_yoy_matrix.csv',
    stock_pool='all',           # all/microcap/zz1000
    enable_cost=True,           # 是否计算交易成本
    n_groups=10,                # 分组数
    start_date='2015-01-01',    # 开始日期
    end_date='2024-12-31'       # 结束日期
)
```

### 批量分析

```python
from factor_engine.backtest import run_factor_analysis_batch

factors = [
    {'name': 'NetProfitYoY', 'matrix_file': 'n_income_attr_p_yoy_matrix.csv'},
    {'name': 'RevenueYoY', 'matrix_file': 'total_revenue_yoy_matrix.csv'},
]

run_factor_analysis_batch(
    factors=factors,
    stock_pools=['all', 'microcap'],
    enable_cost_options=[False, True]
)
```

### 现有分析脚本

```bash
# 净利润YoY
python scripts/factor_analysis/analyze_net_profit_yoy_factor.py

# 营收YoY
python scripts/factor_analysis/analyze_revenue_yoy_factor.py

# PB因子
python scripts/factor_analysis/analyze_pb_factor.py

# 微盘股内分析
python scripts/factor_analysis/analyze_factors_in_microcap.py
```

---

## 新增因子

### 1. 新增矩阵构建脚本（使用基类）

创建 `scripts/matrix_building/build_my_factor.py`：

```python
#!/usr/bin/env python
from data_engine.processors import PriceBasedFactorBuilder
from utils import setup_logger

class MyFactorBuilder(PriceBasedFactorBuilder):
    def get_factor_definitions(self):
        return [('my_factor', '我的因子')]

    def calculate_factors_for_stock(self, ts_code, close, **kwargs):
        # 计算因子值
        return {'my_factor': close.pct_change(20) * 100}

if __name__ == '__main__':
    MyFactorBuilder(logger=setup_logger()).build_and_save()
```

### 2. 新增因子分析脚本

创建 `scripts/factor_analysis/analyze_my_factor.py`：

```python
#!/usr/bin/env python
from factor_engine.backtest import run_single_factor_analysis
from utils import setup_logger

if __name__ == '__main__':
    logger = setup_logger()
    
    for enable_cost in [False, True]:
        run_single_factor_analysis(
            factor_name='MyFactor',
            factor_matrix_file='my_factor_matrix.csv',
            enable_cost=enable_cost,
            logger=logger
        )
```

---

## 常见问题

### Q1: 如何只更新部分数据？

```bash
# 只更新日线数据（增量）
python scripts/data_fetching/fetch_daily_data.py

# 只更新某只股票的矩阵
data_engine/processors/data_loader.py 中的 build_matrix_from_extractor
```

### Q2: 如何查看分析结果？

```
stockdata/factor_analysis_results/
├── all_stocks/           # 全市场分析
│   └── netprofityoy_no_cost/
│       ├── *.html        # 可视化图表
│       └── *.csv         # 统计数据
└── microcap/             # 微盘股分析
```

### Q3: 如何修改交易成本？

编辑 `config/config.py`：
```python
COMMISSION_RATE = 0.00025      # 佣金
STAMP_DUTY_RATE = 0.001        # 印花税
SLIPPAGE_RATE = 0.001          # 滑点
```

### Q4: 如何添加新的股票池？

在 `factor_analysis_runner.py` 的 `_filter_stock_pool` 中添加。

---

## API参考

### 数据加载

```python
from data_engine.processors.data_loader import (
    load_standard_matrices,  # 加载4个标准矩阵
    align_matrices,          # 批量对齐
    get_all_trading_dates,   # 获取交易日
    get_all_stocks,          # 获取股票列表
)

# 加载并对齐
factor, mv, returns, tradable = load_standard_matrices(
    'my_factor_matrix.csv'
)
factor, mv, returns, tradable = align_matrices(
    factor, mv, returns, tradable
)
```

### 矩阵构建基类

```python
from data_engine.processors import (
    FactorMatrixBuilder,      # 通用基类
    PriceBasedFactorBuilder,  # 仅需要价格
    OHLCVFactorBuilder,       # 需要OHLCV
)
```

---

*更多架构细节请查看 [ARCHITECTURE.md](ARCHITECTURE.md)*
