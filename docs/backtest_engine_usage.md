# BacktestEngine 使用文档

基于 `FactorMatrix` 的回测引擎，提供清晰的 API 和完整的功能。

## 核心设计

```
FactorMatrix (因子/收益/可交易性/市值/指数成分)
         ↓
BacktestEngine (配置 → 对齐 → 筛选 → 分组 → 计算 → 输出)
         ↓
BacktestResult (分组收益、风险指标、IC分析)
```

## 快速开始

```python
from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig

# 1. 加载矩阵
factor = FactorMatrix.load_csv('factor_matrix.csv', name='pb')
returns = FactorMatrix.load_csv('returns_matrix.csv', name='returns')
tradable = FactorMatrix.load_csv('tradable_matrix.csv', name='tradable')
mv = FactorMatrix.load_csv('circ_mv_matrix.csv', name='mv')

# 2. 创建引擎
engine = BacktestEngine(
    config=BacktestConfig(
        n_groups=10,          # 10分组
        enable_cost=False,    # 不含成本
        long_short=True       # 计算多空
    )
)

# 3. 加载数据
engine.load_factor(factor)
engine.load_returns(returns)
engine.load_tradable(tradable)
engine.load_mv(mv)

# 4. 运行回测
result = engine.run()

# 5. 保存结果
result.save(output_dir)
```

## 股票池筛选

支持指数成分股筛选：

```python
# 加载中证1000成分股矩阵
zz1000 = FactorMatrix.load_csv('zz1000_matrix.csv', name='zz1000')

# 加载到引擎
engine.load_index_component(zz1000, name='zz1000')

# 回测时自动只选择成分股
result = engine.run()
```

## 回测结果

```python
result.stats_equal      # 等权风险指标 DataFrame
result.stats_mv         # 市值加权风险指标 DataFrame
result.group_returns_equal  # 等权分组收益
result.ic_series        # IC时间序列
result.ic_mean          # IC均值
result.ic_ir            # IC_IR
```

## 完整示例

见 `examples/backtest_engine_demo.py`
