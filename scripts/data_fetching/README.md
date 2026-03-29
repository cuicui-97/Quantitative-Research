# 数据获取脚本

本目录包含所有从Tushare API获取数据的脚本。

## 脚本列表

### 1. check_api_health.py
**功能**: 检查Tushare API连接状态和积分余额

**使用**:
```bash
python scripts/data_fetching/check_api_health.py
```

**输出**: 控制台显示API状态和可用积分

---

### 2. fetch_basic_data.py
**功能**: 获取股票基本信息（股票代码、名称、上市日期等）

**使用**:
```bash
python scripts/data_fetching/fetch_basic_data.py
```

**输出**: `data/basic/stock_basic.csv`

**字段**:
- ts_code: 股票代码
- symbol: 股票简称
- name: 股票名称
- list_date: 上市日期
- delist_date: 退市日期

---

### 3. fetch_daily_data.py
**功能**: 获取股票日线数据（开高低收、成交量、成交额等）

**使用**:
```bash
# 获取所有股票数据
python scripts/data_fetching/fetch_daily_data.py

# 指定日期范围
python scripts/data_fetching/fetch_daily_data.py --start-date 20200101 --end-date 20201231

# 强制刷新
python scripts/data_fetching/fetch_daily_data.py --force-refresh
```

**输出**: `data/daily/{ts_code}.csv`（每只股票一个文件）

**字段**:
- trade_date: 交易日期
- open: 开盘价（后复权）
- high: 最高价（后复权）
- low: 最低价（后复权）
- close: 收盘价（后复权）
- volume: 成交量
- amount: 成交额

**特点**:
- 支持断点续传
- 自动处理复权
- 每50只股票保存一次临时文件

---

### 4. fetch_st_status.py
**功能**: 获取股票ST状态数据

**使用**:
```bash
python scripts/data_fetching/fetch_st_status.py
```

**输出**: `data/supplementary/st_status.csv`

**字段**:
- ts_code: 股票代码
- trade_date: 交易日期
- status: ST状态（1=ST，0=非ST）

---

### 5. fetch_suspension_status.py
**功能**: 获取股票停牌状态数据

**使用**:
```bash
python scripts/data_fetching/fetch_suspension_status.py
```

**输出**: `data/supplementary/suspension_status.csv`

**字段**:
- ts_code: 股票代码
- trade_date: 交易日期
- suspend_type: 停牌类型
- suspend_reason: 停牌原因

---

### 6. fetch_limit_prices.py
**功能**: 获取股票涨跌停价格数据

**使用**:
```bash
python scripts/data_fetching/fetch_limit_prices.py
```

**输出**: `data/supplementary/limit_prices.csv`

**字段**:
- ts_code: 股票代码
- trade_date: 交易日期
- up_limit: 涨停价
- down_limit: 跌停价

**用途**: 用于判断股票是否涨跌停

---

### 7. fetch_fama_data.py
**功能**: 获取Fama-French三因子数据

**使用**:
```bash
python scripts/data_fetching/fetch_fama_data.py
```

**输出**:
- `data/supplementary/fama_factor_mkt.csv` - 市场因子
- `data/supplementary/fama_factor_smb.csv` - 规模因子
- `data/supplementary/fama_factor_hml.csv` - 价值因子

---

### 8. validate_fama_data.py
**功能**: 验证Fama数据的完整性和一致性

**使用**:
```bash
python scripts/data_fetching/validate_fama_data.py
```

**输出**: 控制台显示验证结果

---

## 通用特性

### 增量更新
所有fetch脚本都支持增量更新，重复运行只会获取缺失的数据：
- 检查已有数据的日期范围
- 自动识别缺失的历史数据和最新数据
- 合并、去重、排序后保存

### 断点续传
长时间任务（如fetch_daily_data.py）支持断点续传：
- 定期保存临时文件
- 中断后重新运行会从断点继续
- 完成后自动删除临时文件

### 错误处理
- 自动重试失败的API调用（最多3次）
- 详细的日志输出
- 优雅处理用户中断（Ctrl+C）

## 执行顺序建议

首次运行时按以下顺序执行：

1. `check_api_health.py` - 确保API可用
2. `fetch_basic_data.py` - 获取股票列表
3. `fetch_daily_data.py` - 获取日线数据（耗时最长）
4. `fetch_st_status.py` - 获取ST状态
5. `fetch_suspension_status.py` - 获取停牌状态
6. `fetch_limit_prices.py` - 获取涨跌停价格

后续更新时，只需运行需要更新的脚本即可。

## 注意事项

1. **API积分**: 确保Tushare账号有足够积分
2. **网络稳定**: 长时间任务建议在网络稳定时运行
3. **磁盘空间**: 日线数据约占用几百MB空间
4. **运行环境**: 必须在 `stock` conda环境下运行
