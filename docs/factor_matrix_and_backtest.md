# FactorMatrix + BacktestEngine 使用文档

## 文件结构

```
stock/
├── data_engine/core/
│   ├── __init__.py
│   └── factor_matrix.py          # 矩阵数据结构（内部 DataFrame）
├── factor_engine/backtest/
│   ├── backtest_engine.py        # 回测引擎（含可视化）
│   ├── ...                       # 其他原有模块
├── examples/
│   ├── quick_start.py            # 快速开始示例
│   └── backtest_engine_demo.py   # 完整示例
└── docs/
    └── factor_matrix_and_backtest.md  # 本文档
```

## FactorMatrix

### 核心设计

内部使用 **pandas DataFrame** 存储，提供统一接口：

```python
@dataclass
class FactorMatrix:
    name: str              # 因子名称
    data: pd.DataFrame     # (dates × stocks) float32
    description: str       # 描述
```

### 基本用法

```python
from data_engine.core.factor_matrix import FactorMatrix

# 从 CSV 加载
fm = FactorMatrix.from_csv('pb_matrix.csv', name='pb')

# 查看信息
print(fm)                    # FactorMatrix('pb', shape=(2724, 6363))
print(fm.shape)             # (2724, 6363)
print(fm.info())            # {'name': 'pb', 'shape': (2724, 6363), ...}

# 访问底层 DataFrame
df = fm.data                # 直接访问
df = fm.to_pandas()         # 显式转换
arr = fm.to_numpy()         # 转为 NumPy
```

### 矩阵运算

```python
# 截面排名
ranked = fm.rank(axis=1)           # pct=True 默认

# 截面标准化 (z-score)
zscore = fm.zscore(axis=1)

# 时间序列运算
chg = fm.pct_change(periods=20)    # 20日变化
shifted = fm.shift(1)              # 下移1期
ma20 = fm.rolling_mean(20)         # 20日均值

# 算术运算
combined = (fm1.rank() + fm2.rank()) / 2
scaled = fm * 2 - 1

# 对齐（取交集）
fm1_aligned, fm2_aligned = fm1.align(fm2)
```

### 切片筛选

```python
# 日期切片
fm_2020 = fm.slice_dates(start='20200101', end='20231231')

# 股票切片
fm_selected = fm.slice_stocks(['000001.SZ', '000002.SZ'])
```

## BacktestEngine

### 核心设计

```
FactorMatrix (因子/收益/可交易性/市值/指数成分)
         ↓
BacktestEngine (对齐 → 筛选 → 分组 → 计算 → 输出)
         ↓
BacktestResult (数据 + 图表)
```

### 快速开始

```python
from data_engine.core.factor_matrix import FactorMatrix
from factor_engine.backtest.backtest_engine import BacktestEngine, BacktestConfig

# 1. 加载矩阵
factor = FactorMatrix.from_csv('pb_matrix.csv', name='pb')
returns = FactorMatrix.from_csv('returns_matrix.csv', name='returns')
tradable = FactorMatrix.from_csv('tradable_matrix.csv', name='tradable')
mv = FactorMatrix.from_csv('circ_mv_matrix.csv', name='mv')

# 2. 创建引擎
engine = BacktestEngine(
    config=BacktestConfig(
        n_groups=10,          # 10分组
        enable_cost=False,    # 不含交易成本
        long_short=True       # 计算多空组合
    )
)

# 3. 加载矩阵
engine.load_factor(factor)
engine.load_returns(returns)
engine.load_tradable(tradable)
engine.load_mv(mv)

# 4. 运行回测
result = engine.run()

# 5. 生成完整报告（数据 + 图表）
result.generate_report(output_dir)
```

### 输出结果

```python
# 数据文件
result.save(output_dir)
# 生成:
#   - {factor}_group_returns_equal.csv
#   - {factor}_group_returns_mv.csv
#   - {factor}_stats_equal.csv
#   - {factor}_stats_mv.csv
#   - {factor}_ic_series.csv

# 可视化图表
result.plot_cumulative_returns('cumulative.png')
result.plot_group_statistics('statistics.png')

# 完整报告（数据+图表）
result.generate_report(output_dir)
```

### 股票池筛选

```python
# 加载指数成分股矩阵
zz1000 = FactorMatrix.from_csv('中证1000_matrix.csv', name='zz1000')

# 加载到引擎
engine.load_factor(factor)
engine.load_returns(returns)
engine.load_tradable(tradable)
engine.load_mv(mv)
engine.load_index_component(zz1000, name='zz1000')  # 限定股票池

# 回测时自动只选择中证1000成分股
result = engine.run()
```

### BacktestResult 属性

```python
result.factor_name          # 因子名称
result.config               # 回测配置

# 收益数据
result.group_returns_equal  # 等权分组收益 (dates × groups)
result.group_returns_mv     # 市值加权分组收益
result.cumulative_equal     # 累计收益
result.cumulative_mv

# 风险指标
result.stats_equal          # 等权统计指标 DataFrame
result.stats_mv             # 市值加权统计指标
# 指标包括: mean_return, std, sharpe, win_rate, cumulative_return, annual_return, max_drawdown

# IC分析
result.ic_series            # IC时间序列
result.ic_mean              # IC均值
result.ic_std               # IC标准差
result.ic_ir                # IC_IR
result.ic_win_rate          # IC胜率

# 汇总信息
print(result.summary())     # 打印回测摘要
```

## 与原有系统对比

| 特性 | 原有系统 | 新 FactorMatrix + BacktestEngine |
|-----|---------|--------------------------------|
| 数据结构 | DataFrame | FactorMatrix (封装 DataFrame) |
| 矩阵运算 | 直接 pandas | fm.rank(), fm.zscore() 等方法 |
| 回测入口 | SingleFactorAnalyzer | BacktestEngine |
| 可视化 | FactorVisualizer 类 | BacktestResult 内置方法 |
| 输出方式 | 分别调用 | result.generate_report() 一键生成 |
| 指数筛选 | 手动处理 | engine.load_index_component() |

## 性能

数据规模 2700 天 × 5000 股票：

| 操作 | 耗时 |
|-----|------|
| 加载 CSV | ~1-2 秒 |
| 截面排名 | ~1 秒 |
| 对齐 | ~50 ms |
| 完整回测 | ~20-30 秒 |

## 示例文件

- `examples/quick_start.py` - 最简单的使用方式
- `examples/backtest_engine_demo.py` - 完整功能演示

运行示例：

```bash
# 快速开始
python examples/quick_start.py

# 完整演示
python examples/backtest_engine_demo.py
```
