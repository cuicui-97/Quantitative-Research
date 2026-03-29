# 因子分析脚本

本目录包含因子回测分析的脚本。

## 什么是因子分析？

因子分析是量化投资的核心方法：
1. **因子**: 影响股票收益的特征（如PB、市值、动量等）
2. **分组回测**: 按因子值分组，观察不同组的收益表现
3. **验证有效性**: 判断因子是否能预测未来收益

## 分析框架

### 标准流程
```
T日收盘 → 计算因子值 → 分成10组 → 过滤可交易 → T+1日开盘建仓 → 计算收益
```

### 关键要点
- ✅ **前视性**: T日信号 → T+1日收益（避免未来函数）
- ✅ **可交易性**: 过滤ST、停牌、涨跌停、新股等
- ✅ **等权 vs 市值加权**: 两种收益计算方式
- ✅ **多空组合**: Group 10 - Group 1

---

## 脚本列表

### 1. analyze_pb_factor.py
**功能**: PB因子（市净率）回测分析

**使用**:
```bash
python scripts/factor_analysis/analyze_pb_factor.py
```

**输出**: `factor_analysis_results/pb_factor/`
- `pb_factor_statistics_equal.csv` - 等权统计指标
- `pb_factor_statistics_mv_weighted.csv` - 市值加权统计指标
- `pb_factor_group_returns_equal.csv` - 等权收益时间序列
- `pb_factor_group_returns_mv_weighted.csv` - 市值加权收益时间序列
- 4张可视化图表

**核心逻辑**:
1. 加载PB矩阵、收益率矩阵、可交易矩阵、市值矩阵
2. 每日过滤T+1日可交易股票
3. 在可交易股票中按PB值分成10组（小→大）
4. 计算每组T+1日的收益（等权 + 市值加权）
5. 统计分析和可视化

**关键发现**:
- 高PB股票表现优于低PB股票
- 与价值投资理念相反，体现成长股溢价
- 市值加权下，低PB组表现改善（小市值风险高）

---

### 2. analyze_mv_factor.py
**功能**: 市值因子回测分析

**使用**:
```bash
python scripts/factor_analysis/analyze_mv_factor.py
```

**输出**: `factor_analysis_results/mv_factor/`
- 同PB因子，8个文件（4个CSV + 4个图表）

**核心逻辑**:
1. 加载流通市值矩阵、收益率矩阵、可交易矩阵
2. 每日过滤T+1日可交易股票
3. 在可交易股票中按市值分成10组（小→大）
4. 计算每组T+1日的收益（等权 + 市值加权）
5. 统计分析和可视化

**关键发现**:
- 大市值股票表现优于小市值股票
- "小市值效应"在A股不成立（至少在该时期）
- 等权与市值加权结果相近（市值因子内部一致）

---

### 3. analyze_tradability_matrix.py
**功能**: 可交易性矩阵分析

**使用**:
```bash
python scripts/factor_analysis/analyze_tradability_matrix.py
```

**输出**: 控制台显示统计信息
- 可交易比例
- 各条件的不可交易比例
- 时间序列变化

**用途**: 诊断可交易性过滤的影响

---

## 输出文件说明

### 统计指标CSV
包含以下指标（10组 + 1个多空组合）：

| 指标 | 说明 | 单位 |
|------|------|------|
| mean_return | 日均收益 | % |
| std_return | 收益波动率 | % |
| sharpe_ratio | 夏普比率 | - |
| win_rate | 胜率 | % |
| cumulative_return | 累计收益 | % |

### 收益时间序列CSV
矩阵格式（日期 × 组别）：
- 行：交易日期
- 列：10个分组
- 值：该日该组的平均收益

### 可视化图表

#### 累计收益曲线图
- X轴：交易日序号
- Y轴：累计收益倍数
- 10条彩色曲线（Group 1-10）
- 1条黑色虚线（多空组合）

#### 统计指标柱状图（2×2布局）
- 左上：平均日收益
- 右上：夏普比率
- 左下：胜率
- 右下：累计收益

---

## 分析方法详解

### 1. 分组逻辑

```python
# 伪代码
for each_date:
    # 1. 获取T日因子值
    factor_values = factor_matrix[date]

    # 2. 获取T+1日可交易状态
    tradable_status = tradability_matrix[date+1]

    # 3. 过滤：只保留T+1日可交易的股票
    valid_stocks = stocks where (tradable_status == 0) and (factor_values != NaN)

    # 4. 在可交易股票中按因子值分成10组
    groups = qcut(factor_values[valid_stocks], q=10)
```

**为什么先过滤再分组？**
- 确保各组股票池一致
- 避免不同组因可交易性差异导致的不公平比较

### 2. 收益计算

#### 等权收益
```python
group_return[date, group] = mean(returns[date+1, group_stocks])
```

#### 市值加权收益
```python
weights = market_cap[date, group_stocks] / sum(market_cap[date, group_stocks])
group_return[date, group] = sum(returns[date+1, group_stocks] × weights)
```

### 3. 统计指标

#### 夏普比率
```
Sharpe = (mean_return / std_return) × sqrt(252)
```
- 衡量风险调整后收益
- 年化处理（252个交易日）

#### 胜率
```
Win Rate = (正收益天数 / 总交易天数) × 100%
```

#### 累计收益
```
Cumulative Return = prod(1 + daily_returns) - 1
```

---

## 使用示例

### 完整分析流程

```bash
# 1. 确保已构建矩阵
python scripts/matrix_building/build_all_matrices.py

# 2. 运行因子分析
python scripts/factor_analysis/analyze_pb_factor.py
python scripts/factor_analysis/analyze_mv_factor.py

# 3. 查看结果
ls factor_analysis_results/pb_factor/
ls factor_analysis_results/mv_factor/
```

### 解读结果

查看统计指标CSV：
```bash
# PB因子 - 等权收益
cat factor_analysis_results/pb_factor/pb_factor_statistics_equal.csv
```

关注指标：
- Group 1（低因子值）vs Group 10（高因子值）
- Long-Short（多空组合）的Sharpe比率和胜率
- 累计收益是否呈现单调性

---

## 添加新因子

### 步骤

1. **准备因子矩阵**
   - 在 `matrix_building/` 添加构建脚本
   - 输出到 `data/matrices/{factor_name}_matrix.csv`

2. **复制模板**
   ```bash
   cp scripts/factor_analysis/analyze_pb_factor.py \
      scripts/factor_analysis/analyze_{new_factor}_factor.py
   ```

3. **修改关键部分**
   - 修改 `load_data()`: 加载新因子矩阵
   - 修改输出路径: `factor_analysis_results/{new_factor}_factor/`
   - 修改图表标题

4. **运行分析**
   ```bash
   python scripts/factor_analysis/analyze_{new_factor}_factor.py
   ```

### 示例：动量因子

```python
def load_data(logger):
    """加载所需矩阵"""
    logger.info("加载矩阵数据...")

    # 加载动量因子矩阵（过去20日收益）
    momentum_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'momentum_20d_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')
    mv_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')

    # ... 对齐逻辑 ...

    return momentum, returns, tradable, mv
```

---

## 性能说明

### 运行时间
- PB因子分析：~30秒
- 市值因子分析：~30秒

### 内存占用
- 峰值：~2GB
- 主要占用：加载4个大矩阵

### 输出大小
- 每个因子：~3MB（CSV + 图表）

---

## 注意事项

1. **前视性检查**: 确保T日信号 → T+1日收益
2. **可交易性过滤**: 必须过滤不可交易股票
3. **数据对齐**: 确保所有矩阵索引一致
4. **结果解读**: 注意区分等权和市值加权的差异
5. **样本偏差**: 留意因可交易性过滤导致的样本减少

---

## 相关文档

- [因子分析结果汇总](../../factor_analysis_results/README.md)
- [矩阵构建说明](../matrix_building/README.md)
- [装饰器使用指南](../../docs/fetcher_decorators_guide.md)
