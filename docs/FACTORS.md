# 量化因子体系文档

> 最后更新: 2026-04-25

## 一、因子概览

目前系统共有 **36个因子矩阵**，分为7大类：

| 类别 | 因子数量 | 主要因子 |
|------|---------|---------|
| 财务报表 YoY | 11 | 净利润YoY、营收YoY、EBITDA YoY等 |
| 动量/反转/波动 | 4 | 5日反转、20日/60日动量、20日波动率 |
| 概念景气 | 3 | 价格景气、资金景气、情绪景气 |
| 估值 | 4 | PB、流通市值、总市值、微市值 |
| 状态因子 | 5 | ST状态、可交易性、上市天数等 |
| 流动性 | 4 | Amihud、换手率、价量相关等 |
| 财务单季 | 2 | 净利润单季、EBITDA单季 |

---

## 二、财务报表因子（YoY同比增速）

### 2.1 因子列表

| 因子名 | 矩阵文件 | 数据来源 | 说明 |
|--------|---------|---------|------|
| **净利润YoY** | `n_income_attr_p_yoy_matrix.csv` | income.csv | 归母净利润同比增速 |
| **总利润YoY** | `n_income_yoy_matrix.csv` | income.csv | 净利润同比增速（含少数股东） |
| **营业总收入YoY** | `total_revenue_yoy_matrix.csv` | income.csv | 营业总收入同比增速 |
| **营业收入YoY** | `revenue_yoy_matrix.csv` | income.csv | 营业收入同比增速 |
| **营业利润YoY** | `operate_profit_yoy_matrix.csv` | income.csv | 营业利润同比增速 |
| **利润总额YoY** | `total_profit_yoy_matrix.csv` | income.csv | 利润总额同比增速 |
| **EBITDA YoY** | `ebitda_yoy_matrix.csv` | income.csv | EBITDA同比增速 |
| **管理费用YoY** | `admin_exp_yoy_matrix.csv` | income.csv | 管理费用同比增速 |
| **销售费用YoY** | `sell_exp_yoy_matrix.csv` | income.csv | 销售费用同比增速 |
| **财务费用YoY** | `fin_exp_yoy_matrix.csv` | income.csv | 财务费用同比增速 |
| **归母净利润YoY** | `net_profit_yoy_matrix.csv` | income.csv | 历史遗留，同净利润YoY |

### 2.2 生成方法

**数据来源**: Tushare `income` API（利润表PIT数据）

**PIT处理逻辑**:
```
1. 加载 income.csv，包含字段：ts_code, ann_date, end_date, type, 财务字段
2. type优先级：4(年报)>1(一季报)，3(三季报)>2(中报) —— 累计值
             3>2 —— 单季值
3. 对每个股票，按ann_date分组，取最大end_date的最新报告
4. 计算同比增速：
   YoY = (本期值 - 去年同期值) / abs(去年同期值) * 100
5. PIT展开：从公告日开始，到下一个公告日前一天，使用当前值
6. 截面Winsorize：每日取1%-99%分位数去极值
```

**关键特点**:
- 真实PIT：使用ann_date（公告日）而非end_date（报告期截止日）
- 避免未来数据：T日只能使用已公告的财务数据
- 类型优先级：同一只股票同一天可能有多个report_type，取优先级最高的

### 2.3 回测表现（全市场，等权Long-Short）

| 因子 | 年化收益 | 夏普比率 | 最大回撤 |
|------|---------|---------|---------|
| **净利润YoY** | **18.31%** | **1.79** | -19.19% |
| **营收YoY** | 12.36% | **1.40** | -13.28% |
| **EBITDA YoY** | 3.31% | 0.51 | -26.58% |

**结论**: 净利润YoY和营收YoY表现最佳，夏普>1.3，是核心财务因子。

---

## 三、动量/反转/波动率因子

### 3.1 因子列表

| 因子名 | 矩阵文件 | 计算方法 | 方向 |
|--------|---------|---------|------|
| **5日反转** | `reversal_5d_matrix.csv` | -过去5日累计收益率 | 反向 |
| **20日动量** | `momentum_20d_matrix.csv` | 过去20日累计收益率 | 正向 |
| **60日动量** | `momentum_60d_matrix.csv` | 过去60日累计收益率 | 正向 |
| **20日波动率** | `volatility_20d_matrix.csv` | 过去20日收益率标准差 | - |

**构建脚本**: `build_momentum_matrices.py`

### 3.2 生成方法

```python
# 从日线数据计算
reversal_5d = -(close.pct_change(5))       # 短期反转
momentum_20d = close.pct_change(20)        # 中期动量
momentum_60d = close.pct_change(60)        # 长期动量
volatility_20d = returns.rolling(20).std() # 波动率
```

### 3.3 回测表现

| 因子 | 年化收益 | 夏普比率 | 特点 |
|------|---------|---------|------|
| **5日反转** | **250.53%** | **5.20** | 微盘股内表现极强 |
| **20日动量** | - | - | 中期趋势跟踪 |
| **60日动量** | - | - | 长期趋势跟踪 |

**注意**: 5日反转在微盘股内表现极强，但在全市场效果一般。

---

## 四、概念景气因子

### 4.1 因子列表

| 因子名 | 矩阵文件 | 计算方法 | 数据来源 |
|--------|---------|---------|---------|
| **概念价格景气** | `concept_price_boom_matrix.csv` | 概念成分股5日平均收益率的均值 | concept_stock_matrix |
| **概念资金景气** | `concept_flow_boom_matrix.csv` | 概念成分股成交额/20日均值的均值 | concept_stock_matrix |
| **概念情绪景气** | `concept_sentiment_matrix.csv` | (上涨占比 + 涨停占比) / 2 | concept_stock_matrix |

### 4.2 生成方法

**概念矩阵**: `concept_stock_matrix.csv` (日期×股票，值为逗号分隔的概念代码)

```python
# 概念价格景气
for each date:
    for each concept:
        concept_stocks = 属于该概念的所有股票
        stock_momentum = 过去5日平均收益率
        concept_price_boom = mean(stock_momentum[concept_stocks])
    # 多概念股票取各概念景气度的平均

# 概念资金景气
for each date:
    for each concept:
        concept_flow = mean(amount / amount_ma20)

# 概念情绪景气
sentiment = (up_ratio + limit_ratio) / 2
up_ratio = 上涨股票数 / 总股票数
limit_ratio = 涨停股票数 / 总股票数
```

### 4.3 PIT正确性

- 概念归属关系按每日实际关系计算
- 股票上市前不含该概念成分股（list_date处理）
- 概念变更当日生效

---

## 五、估值因子

| 因子名 | 矩阵文件 | 计算方法 |
|--------|---------|---------|
| **市净率PB** | `pb_matrix.csv` | daily_basic.pb |
| **流通市值** | `circ_mv_matrix.csv` | daily_basic.circ_mv（万元） |
| **总市值** | `total_mv_matrix.csv` | daily_basic.total_mv（万元） |
| **微市值** | `microcap_matrix.csv` | 市值排名后20%的股票标记为1 |

**数据来源**: Tushare `daily_basic` API

---

## 六、流动性因子

| 因子名 | 矩阵文件 | 计算方法 | 方向 |
|--------|---------|---------|------|
| **Amihud_20d** | `amihud_20d_matrix.csv` | 20日平均(\|收益\|/成交额)×1e9 | 非流动性↑ |
| **换手率_20d** | `turnover_20d_matrix.csv` | 20日平均换手率 | 流动性↑ |
| **换手率波动** | `turnover_vol_20d_matrix.csv` | 20日换手率标准差 | 波动↑ |
| **价量相关系数** | `vp_corr_20d_matrix.csv` | 20日成交量与收盘价相关系数 | - |

**构建脚本**: `build_liquidity_matrices.py`

### 回测表现（微盘股内）

| 因子 | 年化收益 | 夏普比率 | 说明 |
|------|---------|---------|------|
| **Amihud** | **95.91%** | **3.46** | 非流动性因子，值越大越难交易 |
| **换手率** | -48.78% | -2.83 | 负向因子，高换手股票收益更低 |

---

## 七、状态因子

| 因子名 | 矩阵文件 | 说明 |
|--------|---------|------|
| **ST状态** | `st_matrix.csv` | 1=ST/*ST，0=正常 |
| **可交易性** | `tradability_matrix.csv` | 1=不可交易，0=可交易 |
| **上市天数** | `listing_days_matrix.csv` | 距上市日期的交易日数 |
| **停牌** | `suspension_matrix.csv` | 1=停牌，0=正常 |
| **涨跌停** | `limit_matrix.csv` | 1=涨跌停，0=正常 |

**可交易性综合判断**:
```
不可交易 = 上市天数<180 或 北交所 或 ST 或 数据缺失 或 涨跌停
```

---

## 八、指数成分股矩阵

| 指数 | 矩阵文件 | 构建方法 |
|------|---------|---------|
| **中证1000** | `中证1000_matrix.csv` | Tushare index_weight接口，真实成分股数据 |
| **ZZ1000** | `zz1000_matrix.csv` | 同中证1000（文件名不同） |

**构建脚本**: `build_index_constituent_matrix.py`

**特点**:
- 使用Tushare `pro.index_weight`接口获取真实成分股数据
- 每月末更新成分股列表
- 时变矩阵：1表示当日是成分股，0表示不是

---

## 九、因子组合

### 9.1 组合方法

| 方法 | 说明 | 脚本 |
|------|------|------|
| **等权组合** | 各因子排名后平均 | `combine_factors.py` |
| **IC加权** | 用近期IC均值加权 | `combine_factors.py` |
| **IR加权** | 用近期IR（IC均值/标准差）加权 | `combine_factors.py` |

### 9.2 组合回测（净利润YoY + 营收YoY）

| 组合方法 | 年化收益 | 夏普比率 |
|---------|---------|---------|
| 等权 | - | - |
| IC加权 | - | - |
| IR加权 | - | - |

---

## 十、使用建议

### 10.1 核心因子（推荐优先使用）

| 排名 | 因子 | 理由 |
|------|------|------|
| 1 | **净利润YoY** | 夏普1.79，最稳定的财务因子 |
| 2 | **营收YoY** | 夏普1.40，与净利润互补 |
| 3 | **PB** | 经典估值因子，长期有效 |
| 4 | **5日反转** | 微盘股内夏普5.20，短期效应强 |
| 5 | **Amihud** | 流动性因子（见第六部分），微盘股内夏普3.46 |

### 10.2 不同股票池的表现差异

| 因子 | 全市场夏普 | 微盘股夏普 | 建议股票池 |
|------|-----------|-----------|-----------|
| 净利润YoY | 1.79 | - | 全市场 |
| 5日反转 | - | 5.20 | 微盘股 |
| Amihud | 1.06 | 3.46 | 微盘股 |
| 换手率 | -2.83 | -2.83 | 做空方向 |

### 10.3 注意事项

1. **价量相关系数(vp_corr)**: 全市场内夏普5.89，但可能过拟合，需谨慎
2. **换手率因子**: 负向因子，高换手股票收益更低
3. **概念因子**: 早期数据（1990s）覆盖率低，2015年后数据完整
4. **财务YoY**: 在微盘股内数据覆盖率低（小盘股财报披露质量差），效果不佳
5. **因子覆盖率说明**: 部分因子（如概念、量价）早期数据缺失是正常的，回测时应从2015年开始

---

## 十一、矩阵构建脚本

| 脚本路径 | 功能 |
|---------|------|
| `scripts/matrix_building/build_income_matrices.py` | 财务YoY矩阵（11个因子） |
| `scripts/matrix_building/build_momentum_matrices.py` | 动量/反转/波动率（4个） |
| `scripts/matrix_building/build_liquidity_matrices.py` | 流动性因子（4个） |
| `scripts/matrix_building/build_concept_prosperity_factors.py` | 概念景气（3个） |
| `scripts/matrix_building/build_index_constituent_matrix.py` | 指数成分股矩阵 |
| `scripts/matrix_building/build_valuation_matrices.py` | 估值因子（PB、市值） |
| `scripts/matrix_building/build_all_matrices.py` | 一键构建所有矩阵 |

---

## 十二、因子分析脚本

| 脚本路径 | 功能 |
|---------|------|
| `scripts/factor_analysis/analyze_factor_unified.py` | 统一分析框架，支持多股票池 |
| `scripts/factor_analysis/analyze_all_income_factors.py` | 批量分析所有财务YoY因子 |
| `scripts/factor_analysis/analyze_factors_in_microcap.py` | 微盘股内批量分析 |
| `scripts/factor_analysis/combine_factors.py` | 多因子组合分析 |

---

*文档生成时间: 2026-04-26*
*数据来源: /Users/cuicui/Documents/stockdata/factor_analysis_results/*
