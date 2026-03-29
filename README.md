# A-Share Quantitative Analysis System

基于 Tushare Pro API 的 A 股量化分析系统，包含数据获取、矩阵构建和因子回测三大模块。

## 功能特点

- ✅ **数据获取**: 抓取股票基础信息、日线数据、ST状态、停牌信息等
- ✅ **矩阵构建**: 构建价格、估值、收益率、可交易性等矩阵
- ✅ **因子回测**: PB因子、市值因子等多因子分析框架
- ✅ **增量更新**: 自动识别缺失数据，只获取增量部分
- ✅ **断点续传**: 长时间任务支持中断后继续
- ✅ **双重加权**: 支持等权和市值加权两种收益计算方式
- ✅ **完善日志**: 详细的进度跟踪和错误处理

## 环境要求

- Python 3.10+
- Conda（推荐）
- Tushare Pro API Token（至少 120 积分）

## 快速开始

### 1. 安装环境

```bash
# 创建并激活 Conda 环境
conda env create -f environment.yml
conda activate stock
```

### 2. 配置 API

复制 `.env.example` 为 `.env`，填入你的 Tushare Token：

```bash
cp .env.example .env
# 编辑 .env 文件，填入 TUSHARE_TOKEN
```

### 3. 完整流程

#### 阶段1: 数据获取

```bash
# 检查API连接
python scripts/data_fetching/check_api_health.py

# 获取基础数据
python scripts/data_fetching/fetch_basic_data.py

# 获取日线数据（耗时最长，约30分钟）
python scripts/data_fetching/fetch_daily_data.py

# 获取补充数据
python scripts/data_fetching/fetch_st_status.py
python scripts/data_fetching/fetch_suspension_status.py
python scripts/data_fetching/fetch_limit_prices.py
```

#### 阶段2: 矩阵构建

```bash
# 一键构建所有矩阵（推荐）
python scripts/matrix_building/build_all_matrices.py
```

#### 阶段3: 因子分析

```bash
# PB因子分析
python scripts/factor_analysis/analyze_pb_factor.py

# 市值因子分析
python scripts/factor_analysis/analyze_mv_factor.py
```

查看结果：
```bash
ls /Users/cuicui/Documents/stockdata/factor_analysis_results/pb_factor/
ls /Users/cuicui/Documents/stockdata/factor_analysis_results/mv_factor/
```

## 项目结构

**注意**：为了提升IDE性能，数据文件已独立存储在 `/Users/cuicui/Documents/stockdata/`

```
stock/                                # 代码目录
├── config/                           # 配置文件
│   ├── config.py                     # 主配置（包含数据路径）
│   └── __init__.py
├── data_engine/                      # 数据引擎（核心代码）
│   ├── api/                          # API封装层
│   ├── fetchers/                     # 数据获取层
│   ├── processors/                   # 数据处理层
│   └── utils/                        # 工具函数
├── factor_engine/                    # 因子引擎
│   ├── backtest/                     # 回测模块
│   └── factors/                      # 因子库
├── scripts/                          # 可执行脚本
│   ├── data_fetching/                # 数据获取脚本
│   ├── matrix_building/              # 矩阵构建脚本
│   └── factor_analysis/              # 因子分析脚本
├── environment.yml                   # Conda环境配置
├── requirements.txt                  # Python依赖
├── README.md                         # 本文档
└── PROJECT.md                        # 项目架构文档

stockdata/                            # 数据目录（独立存储）
├── basic/                            # 基础数据
├── daily/                            # 日线数据（按股票，2000+文件）
├── supplementary/                    # 补充数据
├── matrices/                         # 矩阵数据（~1GB）
├── logs/                             # 日志文件
└── factor_analysis_results/          # 因子分析结果
    ├── pb_factor/                    # PB因子结果
    └── mv_factor/                    # 市值因子结果
```

## 核心功能

### 1. 数据获取（data_fetching/）

**支持的数据类型**:
- 股票基础信息（代码、名称、上市日期等）
- 日线数据（开高低收、成交量、成交额）
- ST状态数据
- 停牌信息
- 涨跌停价格
- Fama三因子数据

**特性**:
- 增量更新：自动识别缺失数据
- 断点续传：长任务支持中断后继续
- 错误重试：API失败自动重试（最多3次）

详见 [scripts/data_fetching/README.md](scripts/data_fetching/README.md)

### 2. 矩阵构建（matrix_building/）

**矩阵类型**:

| 类别 | 矩阵 | 说明 |
|------|------|------|
| 价格 | open_matrix, close_matrix | 开盘价、收盘价 |
| 估值 | pb_matrix, circ_mv_matrix | 市净率、流通市值 |
| 收益 | open_return_matrix | 开盘收益率 |
| 可交易性 | tradability_matrix | 综合判断可交易性 |

**矩阵格式**:
- 行：交易日期（按时间排序）
- 列：股票代码（按代码排序）
- 值：该股票在该日期的指标值

详见 [scripts/matrix_building/README.md](scripts/matrix_building/README.md)

### 3. 因子分析（factor_analysis/）

**分析框架**:
```
T日收盘 → 计算因子值 → 分成10组 → 过滤可交易 → T+1日开盘建仓 → 计算收益
```

**已实现因子**:
- **PB因子**: 市净率，低PB vs 高PB
- **市值因子**: 流通市值，小市值 vs 大市值

**输出指标**:
- 日均收益、波动率、夏普比率
- 胜率、累计收益
- 累计收益曲线图
- 统计指标柱状图

**加权方式**:
- 等权：组内股票等权平均
- 市值加权：按流通市值加权

详见 [scripts/factor_analysis/README.md](scripts/factor_analysis/README.md)

## 主要输出

### 数据文件
- `data/basic/stock_basic.csv` - 股票基础信息
- `data/daily/{ts_code}.csv` - 股票日线数据（5800+个文件）
- `data/supplementary/` - ST状态、停牌信息、涨跌停价格等
- `data/matrices/` - 各类矩阵文件（~1GB）

### 分析结果
- `factor_analysis_results/pb_factor/` - PB因子分析结果
  - 统计指标CSV（等权 + 市值加权）
  - 收益时间序列CSV
  - 可视化图表（累计收益曲线、统计柱状图）
- `factor_analysis_results/mv_factor/` - 市值因子分析结果
  - 同上

## 关键发现

### PB因子
- ❌ **价值投资失效**: 低PB股票表现差（等权-68.93%）
- ✅ **成长股溢价**: 高PB股票表现好（等权+3240.80%）
- ⚠️ **小市值风险**: 低PB小市值股票风险极高

### 市值因子
- ❌ **小市值效应不成立**: 小市值股票表现差（等权-57.80%）
- ✅ **大市值优势**: 大市值股票表现好（等权+575.58%）
- 💡 **流动性溢价**: 大市值股票流动性好、风险低

详见 [factor_analysis_results/README.md](factor_analysis_results/README.md)

## 使用建议

### 首次运行
1. 按顺序执行三个阶段（数据获取 → 矩阵构建 → 因子分析）
2. 数据获取阶段耗时最长（~1小时），可以分批运行
3. 矩阵构建一次性完成（~10分钟）
4. 因子分析快速（~1分钟/因子）

### 日常更新
```bash
# 更新日线数据（增量）
python scripts/data_fetching/fetch_daily_data.py

# 更新补充数据
python scripts/data_fetching/fetch_st_status.py
python scripts/data_fetching/fetch_suspension_status.py
python scripts/data_fetching/fetch_limit_prices.py

# 重新构建矩阵
python scripts/matrix_building/build_all_matrices.py

# 重新运行因子分析
python scripts/factor_analysis/analyze_pb_factor.py
python scripts/factor_analysis/analyze_mv_factor.py
```

### 添加新因子
1. 在 `matrix_building/` 构建因子矩阵
2. 复制 `analyze_pb_factor.py` 作为模板
3. 修改因子加载逻辑和输出路径
4. 运行分析并查看结果

## 资源占用

- **磁盘空间**: ~10GB（原始数据 + 矩阵 + 结果）
- **内存**: 峰值 ~3GB（矩阵构建和因子分析时）
- **时间**: 首次运行 ~1.5小时，日常更新 ~15分钟

## 注意事项

1. **API限制**: 确保Tushare积分充足（建议200+）
2. **网络稳定**: 长时间任务建议在网络稳定时运行
3. **前视性**: 所有因子分析严格遵守T日信号→T+1日收益
4. **可交易性**: 已过滤ST、停牌、涨跌停、新股等不可交易情况
5. **数据质量**: 定期检查数据完整性和准确性

## 文档

- [scripts/README.md](scripts/README.md) - 脚本总览
- [scripts/data_fetching/README.md](scripts/data_fetching/README.md) - 数据获取详解
- [scripts/matrix_building/README.md](scripts/matrix_building/README.md) - 矩阵构建详解
- [scripts/factor_analysis/README.md](scripts/factor_analysis/README.md) - 因子分析详解
- [factor_analysis_results/README.md](factor_analysis_results/README.md) - 分析结果说明
- [docs/fetcher_decorators_guide.md](docs/fetcher_decorators_guide.md) - 装饰器使用指南
- [PROJECT.md](PROJECT.md) - 项目架构文档（面向开发者）

## License

MIT License
