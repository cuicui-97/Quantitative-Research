# 矩阵构建脚本

本目录包含将原始数据转换为矩阵格式的脚本。

## 什么是矩阵？

矩阵是因子分析的基础数据结构：
- **行**: 交易日期（按时间排序）
- **列**: 股票代码（按代码排序）
- **值**: 该股票在该日期的某个指标值

**示例**: PB矩阵
```
            000001.SZ  000002.SZ  600000.SH  ...
20200103    8.5        12.3       5.6        ...
20200106    8.6        12.1       5.7        ...
...
```

## 脚本列表

### 1. build_all_matrices.py ⭐ 推荐
**功能**: 一键构建所有矩阵

**使用**:
```bash
# 构建所有矩阵
python scripts/matrix_building/build_all_matrices.py

# 只构建特定矩阵
python scripts/matrix_building/build_all_matrices.py --matrices price valuation

# 指定日期范围
python scripts/matrix_building/build_all_matrices.py --start-date 20200101 --end-date 20201231
```

**输出**: `data/matrices/` 下的所有矩阵文件

**构建的矩阵**:
- 价格矩阵（open, close, high, low, volume, amount）
- 估值矩阵（pb, circ_mv, total_mv）
- 收益率矩阵（open_return, close_return）
- 可交易性矩阵（tradability）

---

### 2. build_trade_calendar.py
**功能**: 构建交易日历矩阵

**使用**:
```bash
python scripts/matrix_building/build_trade_calendar.py
```

**输出**: `data/matrices/trade_calendar.csv`

**用途**: 确定有效交易日，用于其他矩阵构建

---

### 3. build_price_matrices.py
**功能**: 构建价格相关矩阵

**使用**:
```bash
# 构建所有价格矩阵
python scripts/matrix_building/build_price_matrices.py

# 只构建特定矩阵
python scripts/matrix_building/build_price_matrices.py --matrices open close

# 指定日期范围
python scripts/matrix_building/build_price_matrices.py --start-date 20200101
```

**输出**:
- `data/matrices/open_matrix.csv` - 开盘价矩阵
- `data/matrices/close_matrix.csv` - 收盘价矩阵
- `data/matrices/high_matrix.csv` - 最高价矩阵
- `data/matrices/low_matrix.csv` - 最低价矩阵
- `data/matrices/volume_matrix.csv` - 成交量矩阵
- `data/matrices/amount_matrix.csv` - 成交额矩阵

**特点**:
- 使用后复权价格
- 并行处理（4线程）
- 自动对齐到交易日历

---

### 4. build_valuation_matrices.py
**功能**: 构建估值相关矩阵

**使用**:
```bash
# 构建所有估值矩阵
python scripts/matrix_building/build_valuation_matrices.py

# 只构建特定矩阵
python scripts/matrix_building/build_valuation_matrices.py --matrices pb circ_mv
```

**输出**:
- `data/matrices/pb_matrix.csv` - 市净率矩阵
- `data/matrices/circ_mv_matrix.csv` - 流通市值矩阵（万元）
- `data/matrices/total_mv_matrix.csv` - 总市值矩阵（万元）

**数据来源**: `data/supplementary/daily_basic.csv`

---

## 矩阵类型说明

### 价格矩阵
| 矩阵 | 说明 | 用途 |
|------|------|------|
| open_matrix | 开盘价 | 计算开盘收益率 |
| close_matrix | 收盘价 | 计算收盘收益率 |
| high_matrix | 最高价 | 波动率分析 |
| low_matrix | 最低价 | 波动率分析 |
| volume_matrix | 成交量 | 流动性分析 |
| amount_matrix | 成交额 | 流动性分析 |

### 估值矩阵
| 矩阵 | 说明 | 用途 |
|------|------|------|
| pb_matrix | 市净率 | PB因子分析 |
| circ_mv_matrix | 流通市值 | 市值因子分析、市值加权 |
| total_mv_matrix | 总市值 | 市值因子分析 |

### 收益率矩阵
| 矩阵 | 说明 | 计算公式 |
|------|------|----------|
| open_return_matrix | 开盘收益率 | (Open_t - Open_{t-1}) / Open_{t-1} |
| close_return_matrix | 收盘收益率 | (Close_t - Close_{t-1}) / Close_{t-1} |

### 可交易性矩阵
| 矩阵 | 说明 | 值含义 |
|------|------|--------|
| tradability_matrix | 可交易性 | 0=可交易，1=不可交易 |

**综合判断**:
- 上市天数 < 180天 → 不可交易
- ST状态 → 不可交易
- 停牌 → 不可交易
- 涨跌停 → 不可交易
- 数据缺失 → 不可交易

---

## 矩阵构建流程

### 1. 数据读取
- 从 `data/daily/{ts_code}.csv` 读取日线数据
- 从 `data/supplementary/` 读取补充数据

### 2. 矩阵对齐
- 对齐到统一的交易日历
- 对齐到统一的股票列表
- 缺失值填充为NaN

### 3. 数据处理
- 价格数据：使用后复权价格
- 收益率：计算日收益率
- 可交易性：综合多个条件判断

### 4. 保存
- CSV格式保存
- 第一列为日期索引（trade_date）
- 第一行为股票代码

---

## 性能优化

### 并行处理
使用多线程并行处理股票数据：
```python
# 默认4线程
python scripts/matrix_building/build_price_matrices.py --n-jobs 4

# 调整线程数
python scripts/matrix_building/build_price_matrices.py --n-jobs 8
```

### 矩阵大小
典型矩阵大小（2015-2025，约5500只股票）：
- 价格矩阵：~100MB/个
- 估值矩阵：~70MB/个
- 可交易性矩阵：~50MB

总计：~1GB

---

## 使用建议

### 首次运行
```bash
# 推荐：一键构建所有矩阵
python scripts/matrix_building/build_all_matrices.py
```

### 增量更新
矩阵构建脚本会自动检测已有矩阵，只构建缺失的部分。

### 数据验证
构建完成后，脚本会输出统计信息：
- 矩阵维度
- 非空值比例
- 数值范围（最小值、最大值、中位数）

---

## 注意事项

1. **依赖关系**: 必须先运行 `data_fetching/` 中的脚本
2. **内存占用**: 构建过程中峰值内存约2-3GB
3. **磁盘空间**: 所有矩阵约占用1GB空间
4. **运行时间**: 首次构建约需5-10分钟
