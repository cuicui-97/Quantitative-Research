# 因子构建框架使用指南

## 概述

v3.0 框架是一次**全面重构**，解决了以下核心问题：

| 问题 | 旧架构 | v3.0 架构 |
|------|--------|-----------|
| 计算方式 | 逐股票循环（慢） | 矩阵向量化（10x+ 提速） |
| 中间结果 | 重复计算 | 自动缓存复用 |
| 因子定义 | Python 代码 | 声明式 YAML 配置 |
| 数据时点 | 隐式/容易出错 | 显式标记/自动调整 |
| 扩展性 | 需修改代码 | 配置即可 |

---

## 快速开始

### 1. 使用预定义构建器

```python
from data_engine.core import MomentumFactorBuilder, LiquidityFactorBuilder

# 动量因子
builder = MomentumFactorBuilder(start_date='20200101')
results = builder.build()

# 流动性因子
builder = LiquidityFactorBuilder(start_date='20200101')
results = builder.build()
```

### 2. 从 YAML 配置构建

```python
from data_engine.core import UnifiedFactorBuilder

builder = UnifiedFactorBuilder(config_file='config/factors/momentum.yaml')
results = builder.build()
```

### 3. 命令行

```bash
# 从配置构建
python scripts/matrix_building/build_factors.py --config config/factors/momentum.yaml

# 使用预定义构建器
python scripts/matrix_building/build_factors.py --type momentum --start-date 20200101

# 构建全部
python scripts/matrix_building/build_factors.py --type all
```

---

## 声明式配置语法

### 基础因子

```yaml
factors:
  - name: momentum_20d
    description: "20日动量因子"
    formula: "close.pct_change(20)"
    dependencies: ["close"]
    availability: "close"
```

### 方法链调用

```yaml
factors:
  - name: volatility_20d
    description: "20日波动率"
    formula: "close.pct_change().rolling(20).std()"
    dependencies: ["close"]
    availability: "close"
```

### 多数据源

```yaml
factors:
  - name: turnover_daily
    description: "日换手率"
    formula: "amount / (circ_mv.shift(1) * 100 + 1e-10)"
    dependencies: ["amount", "circ_mv"]
    availability: "close"
```

### 依赖其他因子（中间结果复用）

```yaml
factors:
  # 先定义中间结果
  - name: turnover_daily
    description: "日换手率"
    formula: "amount / (circ_mv.shift(1) * 100 + 1e-10)"
    dependencies: ["amount", "circ_mv"]
    availability: "close"

  # 复用中间结果
  - name: turnover_20d
    description: "20日平均换手率"
    formula: "turnover_daily.rolling(20).mean()"
    dependencies: ["turnover_daily"]
    availability: "close"

  - name: turnover_vol_20d
    description: "20日换手率波动率"
    formula: "turnover_daily.rolling(20).std()"
    dependencies: ["turnover_daily"]
    availability: "close"
```

框架会自动识别并缓存 `turnover_daily`，避免重复计算。

---

## 数据时点安全

### 可用时点定义

| 时点 | 含义 | 典型数据 |
|------|------|----------|
| `open` | T日开盘时可用 | 昨日收盘价 |
| `close` | T日收盘后可用 | 当日收盘价、成交量 |
| `next_open` | T+1日开盘前可用 | 日终计算的指标（市值、PE） |

### 自动滞后机制

当因子目标时点与数据实际时点不匹配时，框架自动调整：

```yaml
factors:
  - name: turnover_safe
    description: "安全的换手率"
    formula: "amount / (circ_mv * 100 + 1e-10)"
    dependencies: ["amount", "circ_mv"]
    availability: "open"  # 开盘时可用
```

在此例中：
- `circ_mv` 是 `next_open` 时点（日终计算）
- 因子目标是 `open` 时点
- 框架自动使用 `circ_mv.shift(1)` 确保无未来函数

---

## 从旧架构迁移

### 旧代码（FactorMatrixBuilder）

```python
class MomentumBuilder(FactorMatrixBuilder):
    def get_factor_definitions(self):
        return [
            ('reversal_5d', '5日反转'),
            ('momentum_20d', '20日动量'),
        ]

    def calculate_factors_for_stock(self, ts_code, df):
        close = df['close']
        return {
            'reversal_5d': -close.pct_change(5),
            'momentum_20d': close.pct_change(20),
        }

# 使用
MomentumBuilder().run()
```

### 新代码（UnifiedFactorBuilder）

**方式1：Python 代码**

```python
from data_engine.core import UnifiedFactorBuilder, FactorDefinition, DataAvailability

builder = UnifiedFactorBuilder()

builder.add_factor(FactorDefinition(
    name='reversal_5d',
    description='5日反转',
    formula='-close.pct_change(5)',
    dependencies=['close'],
    availability=DataAvailability.CLOSE
))

builder.add_factor(FactorDefinition(
    name='momentum_20d',
    description='20日动量',
    formula='close.pct_change(20)',
    dependencies=['close'],
    availability=DataAvailability.CLOSE
))

results = builder.build()
```

**方式2：YAML 配置**（推荐）

```yaml
# config/factors/momentum.yaml
factors:
  - name: reversal_5d
    description: "5日反转"
    formula: "-close.pct_change(5)"
    dependencies: ["close"]
    availability: "close"

  - name: momentum_20d
    description: "20日动量"
    formula: "close.pct_change(20)"
    dependencies: ["close"]
    availability: "close"
```

```python
builder = UnifiedFactorBuilder(config_file='config/factors/momentum.yaml')
results = builder.build()
```

---

## 性能对比

### 测试环境
- 数据：5000+ 只股票，2000+ 个交易日
- 因子：4个动量 + 4个流动性因子

### 结果

| 指标 | 旧架构 | v3.0 | 提升 |
|------|--------|------|------|
| 总耗时 | 180s | 15s | **12x** |
| 内存占用 | 低（逐只加载） | 中等（矩阵缓存） | - |
| 中间结果复用 | 无 | 有 | 节省 30%+ |

---

## 支持的公式语法

### 基础运算

```yaml
formula: "close + open"                    # 加法
formula: "close - open"                    # 减法
formula: "close * vol"                     # 乘法
formula: "close / open"                    # 除法
formula: "(close - open) / open"           # 括号
```

### Pandas 方法链

```yaml
formula: "close.pct_change(20)"                           # 收益率
formula: "close.rolling(20).mean()"                        # 移动平均
formula: "close.rolling(20).std()"                         # 移动标准差
formula: "close.rolling(20).corr(vol)"                     # 滚动相关系数
formula: "close.pct_change().rolling(20).std()"            # 方法链
formula: "close.rank(axis=1, pct=True)"                    # 截面排名
formula: "close.shift(1)"                                  # 滞后
```

### NumPy 函数

```yaml
formula: "np.abs(close.pct_change())"                      # 绝对值
formula: "np.log(close)"                                   # 对数
formula: "np.sqrt(close)"                                  # 平方根
formula: "np.sign(close.diff())"                           # 符号
```

---

## 预定义构建器

| 构建器 | 因子 | 说明 |
|--------|------|------|
| `MomentumFactorBuilder` | reversal_5d, momentum_20d, momentum_60d, volatility_20d | 动量/反转/波动率 |
| `LiquidityFactorBuilder` | amihud_20d, turnover_20d, turnover_vol_20d, vp_corr_20d | 流动性指标 |

---

## 故障排查

### 公式求值失败

```
公式求值失败: close.pct_change(20), 错误: 'DataFrame' object has no attribute 'pct_change'
```

**原因**：数据列名不匹配
**解决**：检查 `dependencies` 中的名称是否正确

### 内存不足

```python
# 减少日期范围
builder = UnifiedFactorBuilder(start_date='20220101', end_date='20231231')

# 或者分批构建
builder = UnifiedFactorBuilder()
builder.build(factor_names=['momentum_20d'])  # 只构建一个
```

### 缓存不生效

检查因子依赖关系是否正确声明。中间结果必须在 `dependencies` 中列出才能被复用。

---

## 财务因子构建

### 预计算（首次运行或数据更新时）

```bash
python scripts/data_fetching/build_ann_date_latest_end.py
```

生成 `supplementary/ann_date_latest_end.csv`，供后续所有财务因子构建使用。

### 构建 YoY 因子

```python
from data_engine.processors.financial_matrix_builder import FinancialMatrixBuilder

builder = FinancialMatrixBuilder(start_date='20200101', n_workers=8)

# 归母净利润 YoY
builder.yoy('n_income_attr_p')

# 营收 YoY
builder.yoy('revenue')
```

### 构建其他财务因子

```python
# PIT 单季值
builder.pit_single_quarter('n_income_attr_p')

# PIT 累计值
builder.pit_cumulative('n_income_attr_p')

# TTM
builder.ttm('n_income_attr_p')

# 累计值 YoY（适用于只有累计数据的字段）
builder.yoy_cumulative('ebitda')
```

---

## 完整示例

参见 `examples/unified_factor_builder_demo.py` 和 `config/factors/` 目录。
