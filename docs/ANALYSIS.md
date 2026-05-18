# A股因子分析方案

> 本文档记录因子计算、对齐、回测的完整逻辑，防止未来数据泄露

## 一、核心时间线

```
日期:    T-20 ... T-1   T        T+1       T+2
         │      │      │         │         │
         ▼      ▼      ▼         ▼         ▼
数据:    ──────收盘价数据─────────▶
               │      │
               │      │  T日收盘后计算因子
               │      │  (此时已知T日收盘价)
               │      │
               │      ▼
因子值:         [基于T-19~T数据]
                      │
                      ▼
                T+1日开盘买入
                (按T日因子值分组)
                      │
                      ▼
                T+2日开盘卖出
                收益 = (Open_{T+2} - Open_{T+1}) / Open_{T+1}
```

## 二、关键原则

### 2.1 因子计算时机

**因子值在T日收盘后计算，包含T日数据**。

原因：
- T日收盘后，我们知道T日的收盘价、成交量、成交额等全部数据
- 我们在T日收盘后做研究、决策，准备第二天(T+1)的交易
- 因此因子**应当**包含T日的信息

### 2.2 回测对齐方式

回测引擎使用 `return_matrix.shift(-1)` 将收益对齐：

```python
# 核心逻辑（在 weighting.py 中）
returns_next_day = return_matrix.shift(-1)  # T+1收益移到T日

# 分组收益计算
for g in range(1, n_groups + 1):
    mask = group_matrix == g  # T日的分组
    group_returns[g] = returns_next_day.where(mask).mean(axis=1)  # 取T+1收益
```

### 2.3 为什么不需要 shift(1)

❌ **错误理解**：因子需要用 `shift(1)` 把计算结果往后移一天

✅ **正确理解**：
- T日因子值 = 基于[T-窗口+1, T]的数据计算
- T日分组 → 持有到T+1日 → 收益是T+1日的
- 回测引擎用 `shift(-1)` 把T+1收益对齐到T日
- 逻辑完全自洽，**不需要**在因子计算时加 `shift(1)`

## 三、各类因子计算方式

### 3.1 动量/反转因子

```python
# 20日动量 = 过去20日累计收益（含T日）
momentum_20d = close / close.shift(20) - 1  # 或 close.pct_change(20)

# 5日反转 = -过去5日累计收益（含T日）
reversal_5d = -(close / close.shift(5) - 1)
```

### 3.2 波动率因子

```python
# 20日波动率 = 过去20日收益率标准差（含T日）
volatility_20d = returns.rolling(20).std()
```

### 3.3 换手率因子

```python
# 日换手率 = 成交额 / 流通市值
turnover_daily = amount / (circ_mv * 100)

# 20日平均换手率（含T日）
turnover_20d = turnover_daily.rolling(20).mean()
```

### 3.4 Amihud非流动性

```python
# 日Amihud = |收益率| / 成交额
amihud_daily = np.abs(returns) / amount

# 20日平均Amihud（含T日）
amihud_20d = amihud_daily.rolling(20).mean()
```

### 3.5 价量相关系数

```python
# 20日成交量与收盘价相关系数（含T日）
vp_corr_20d = volume.rolling(20).corr(close)
```

### 3.6 概念/行业动量

```python
# 成分股过去N日平均收益（含T日）
stock_momentum = return_matrix.rolling(window).mean()

# 概念/行业平均
concept_momentum = stock_momentum[成分股].mean(axis=1)
```

### 3.7 财务YoY因子

```python
# PIT展开后，T日因子值 = 最新公告的财务数据计算的YoY
# 已经在数据层面确保PIT正确
yoy_matrix = build_yoy_matrix()  # 详见 financial_matrix_builder.py
```

## 四、未来数据泄露的边界

### 4.1 什么算未来数据泄露？

❌ **错误示例**：
```python
# T日因子用了T+1日才能知道的数据
factor_t = close.shift(-1) / close - 1  # 这是T+1的收益，不是因子！
```

✅ **正确示例**：
```python
# T日因子只用T日及之前的数据
factor_t = close / close.shift(20) - 1  # 过去20日收益，T日收盘后可知
```

### 4.2 常见陷阱

| 陷阱 | 说明 | 正确做法 |
|------|------|---------|
| 使用未来收盘价 | T日因子包含T+1日收盘价 | 只用到T日收盘价 |
| 使用未来财务数据 | T日因子用了未公告的财报 | 使用ann_date<=T的最新财报 |
| 使用未来概念归属 | T日因子用了T+1日才生效的概念 | 使用T日实际概念归属 |

## 五、回测全流程验证

```python
# 假设今天是 2024-01-10

# 1. T日因子计算（收盘后）
factor_0110 = compute_factor(date='2024-01-10')  # 基于2024-01-10及之前数据

# 2. T日分组
# 第10组 = 因子值最高的10%股票

# 3. T+1日开盘买入（2024-01-11）
# 买入价格 = Open_2024-01-11

# 4. T+2日开盘卖出（2024-01-12）
# 卖出价格 = Open_2024-01-12
# 收益 = (Open_2024-01-12 - Open_2024-01-11) / Open_2024-01-11

# 5. 回测引擎如何对齐
# return_matrix[2024-01-11] = (Open_2024-01-11 - Open_2024-01-10) / Open_2024-01-10
# return_matrix[2024-01-12] = (Open_2024-01-12 - Open_2024-01-11) / Open_2024-01-11

# 分组收益计算：
# 第10组在2024-01-10的收益 = return_matrix.loc['2024-01-11', 第10组股票].mean()
# 这就是 shift(-1) 的作用
```

## 六、代码规范

### 6.1 矩阵构建脚本规范

```python
def build_factor_matrix():
    """
    因子定义：
    - X日因子 = 过去N日XXX的平均/标准差/相关系数
    - 使用T日收盘后可获得的数据
    - 用于T+1日开盘交易
    """
    # 正确：使用T日及之前的数据
    factor = close.rolling(window).mean()  # ✅

    # 错误：不需要shift(1)
    factor = close.rolling(window).mean().shift(1)  # ❌ 不需要

    return factor
```

### 6.2 注释模板

```python
"""
构建XXX因子矩阵

因子定义（T日收盘后可计算）：
1. XXX: 过去N日YYY的平均/标准差/相关系数

计算方法：
- XXX = ZZZ.rolling(N).mean()

使用方式：
- T日收盘后计算因子值
- T+1日开盘按因子值分组买入
- 收益通过 return_matrix.shift(-1) 对齐
"""
```

## 七、检查清单

在编写新的矩阵构建脚本前，检查：

- [ ] 因子计算只用到了T日及之前的数据？
- [ ] 没有使用 `shift(1)` 把因子往后移？
- [ ] 概念/行业归属使用的是T日的实际归属？
- [ ] 财务数据使用的是ann_date<=T的最新数据？
- [ ] 文档中说明了"T日收盘后可计算"？

## 八、常见错误

### 错误1：画蛇添足的shift(1)

❌ 错误：
```python
# 误以为需要shift(1)避免未来数据
factor = close.pct_change(20).shift(1)  # 错！
```

✅ 正确：
```python
# T日收盘后就能计算过去20日收益
factor = close.pct_change(20)  # 对！
```

### 错误2：混淆因子计算和收益对齐

❌ 错误：
```python
# 在因子层面对齐收益
factor = close.pct_change(20)
returns = return_matrix.shift(-1)  # 这里多此一举
```

✅ 正确：
```python
# 因子只管计算
factor = close.pct_change(20)
# 对齐交给回测引擎
```

## 九、参考文档

- 收益率矩阵定义：`build_return_matrix.py`
- 回测对齐逻辑：`factor_engine/backtest/weighting.py`
- 单因子分析器：`factor_engine/backtest/single_factor_analyzer.py`

---

**最后更新**：2026-04-26
**核心原则**：因子包含T日数据，回测引擎负责收益对齐
