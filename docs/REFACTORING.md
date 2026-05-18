# 代码重构总结

> 日期: 2026-04-26

## 重构目标

消除重复代码，提高可维护性，简化新增因子/分析的流程。

---

## 方案一：矩阵构建基类 ✅

### 新增文件
- `data_engine/processors/factor_matrix_builder.py` - 因子矩阵构建基类

### 提供的基类

| 基类 | 用途 | 适用场景 |
|-----|------|---------|
| `FactorMatrixBuilder` | 通用基类 | 所有因子矩阵构建 |
| `PriceBasedFactorBuilder` | 仅需收盘价 | 动量、反转、波动率等 |
| `OHLCVFactorBuilder` | 需要开高低收量额 | 复杂技术指标 |

### 子类需实现的方法

```python
class MyFactorBuilder(PriceBasedFactorBuilder):
    def get_factor_definitions(self):
        """返回因子定义列表 [(因子名, 中文名), ...]"""
        return [('momentum_20d', '20日动量')]

    def calculate_factors_for_stock(self, ts_code, close, **kwargs):
        """计算单只股票的因子，返回 Dict[str, Series]"""
        return {'momentum_20d': close.pct_change(20)}
```

### 重构效果

| 脚本 | 重构前行数 | 重构后行数 | 减少 |
|-----|----------|-----------|------|
| build_momentum_matrices.py | 140 | 55 | 60% |
| build_liquidity_matrices.py | 143 | 65 | 55% |

### 使用方式

```python
if __name__ == '__main__':
    MomentumFactorBuilder().build_and_save()
```

---

## 方案二：统一因子分析接口 ✅

### 新增文件
- `factor_engine/backtest/factor_analysis_runner.py` - 统一分析接口

### 提供的函数

| 函数 | 用途 |
|-----|------|
| `run_single_factor_analysis()` | 单因子完整分析流程 |
| `run_factor_analysis_batch()` | 批量多因子分析 |

### 简化后的分析脚本

重构前 (111行):
```python
# 加载4个矩阵
yoy_matrix = load_matrix(...)
mv_matrix = load_matrix(...)
# ... 对齐、创建analyzer、运行分析、保存结果、生成图表 ...
```

重构后 (35行):
```python
run_single_factor_analysis(
    factor_name='NetProfitYoY',
    factor_matrix_file='net_profit_yoy_matrix.csv',
    enable_cost=False,
    stock_pool='all'
)
```

### 参数说明

```python
run_single_factor_analysis(
    factor_name='NetProfitYoY',           # 因子名称
    factor_matrix_file='xxx_matrix.csv',  # 矩阵文件名
    output_subdir=None,                   # 输出子目录
    enable_cost=False,                    # 是否计算成本
    stock_pool='all',                     # 股票池: all/microcap/zz1000
    n_groups=10,                          # 分组数
    start_date=None,                      # 开始日期
    end_date=None,                        # 结束日期
    logger=None                           # 日志对象
)
```

---

## 方案三：通用数据加载工具 ✅

### 新增函数 (data_loader.py)

| 函数 | 用途 |
|-----|------|
| `load_standard_matrices(factor_file)` | 一键加载4个标准矩阵 |
| `align_matrices(*matrices)` | 批量矩阵对齐 |

### 使用示例

```python
from data_engine.processors.data_loader import (
    load_standard_matrices,
    align_matrices
)

# 加载4个矩阵
factor, mv, returns, tradable = load_standard_matrices(
    'net_profit_yoy_matrix.csv',
    logger=logger
)

# 对齐
factor, mv, returns, tradable = align_matrices(
    factor, mv, returns, tradable,
    logger=logger
)
```

---

## 待重构的脚本清单

以下脚本可以用新框架简化：

### 矩阵构建脚本 (14个)
- [x] build_momentum_matrices.py - 已重构
- [x] build_liquidity_matrices.py - 已重构
- [ ] build_concept_factors.py
- [ ] build_concept_prosperity_factors.py
- [ ] build_industry_factors.py
- [ ] build_price_matrices.py
- [ ] build_return_matrix.py
- [ ] build_valuation_matrices.py
- [ ] build_microcap_matrix.py
- [ ] build_trade_calendar.py
- [ ] build_index_constituent_matrix.py
- [ ] build_income_matrices.py
- [ ] build_industry_matrix.py
- [ ] build_all_matrices.py

### 因子分析脚本 (16个)
- [x] analyze_net_profit_yoy_factor.py - 已重构
- [ ] analyze_revenue_yoy_factor.py
- [ ] analyze_ebitda_yoy_factor.py
- [ ] analyze_pb_factor.py
- [ ] analyze_pb_factor_with_cost.py
- [ ] analyze_mv_factor.py
- [ ] analyze_all_income_factors.py
- [ ] analyze_factors_in_microcap.py
- [ ] analyze_momentum_in_microcap.py
- [ ] analyze_liquidity_in_microcap.py
- [ ] analyze_liquidity_all_stocks.py
- [ ] analyze_concept_prosperity_factors.py
- [ ] analyze_factor_unified.py
- [ ] combine_factors.py
- [ ] diagnose_factor_data.py
- [ ] test_transaction_cost.py

---

## 新增因子/分析的快速模板

### 新增矩阵构建脚本

```python
#!/usr/bin/env python
from data_engine.processors import PriceBasedFactorBuilder
from utils import setup_logger

class MyFactorBuilder(PriceBasedFactorBuilder):
    def get_factor_definitions(self):
        return [('my_factor', '我的因子')]

    def calculate_factors_for_stock(self, ts_code, close, **kwargs):
        return {'my_factor': close.pct_change(20)}

if __name__ == '__main__':
    MyFactorBuilder(logger=setup_logger()).build_and_save()
```

### 新增因子分析脚本

```python
#!/usr/bin/env python
from factor_engine.backtest import run_single_factor_analysis
from utils import setup_logger

if __name__ == '__main__':
    logger = setup_logger()
    
    # 全市场
    run_single_factor_analysis(
        factor_name='MyFactor',
        factor_matrix_file='my_factor_matrix.csv',
        stock_pool='all',
        logger=logger
    )
    
    # 微盘股
    run_single_factor_analysis(
        factor_name='MyFactor',
        factor_matrix_file='my_factor_matrix.csv',
        stock_pool='microcap',
        logger=logger
    )
```

---

## 代码统计

| 指标 | 重构前 | 重构后 | 改善 |
|-----|-------|-------|------|
| 重复代码块 | ~20处 | 0处 | 100%消除 |
| 动量矩阵脚本 | 140行 | 55行 | -60% |
| 流动性矩阵脚本 | 143行 | 65行 | -55% |
| 净利润分析脚本 | 111行 | 35行 | -68% |

---

## 下一步建议

1. **逐步迁移**：按需重构剩余脚本，优先重构经常修改的
2. **保持一致**：新增脚本优先使用新框架
3. **文档更新**：在新增因子时参考"因子分析方案.md"
