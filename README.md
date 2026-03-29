# A股量化投资系统

一个完整的A股量化投资数据获取、处理和因子分析系统。

## 项目特点

- 📊 **完整数据流程**：从数据获取到矩阵构建，再到因子分析的完整流程
- 🏗️ **双引擎架构**：数据引擎（data_engine）+ 因子引擎（factor_engine）
- 🔄 **矩阵化计算**：高效的矩阵化数据处理，支持大规模回测
- 📈 **丰富的因子分析**：支持单因子分析，包含12个风险指标
- 🎯 **交易可用性过滤**：综合考虑ST、停牌、涨跌停、上市天数等条件

## 快速开始

### 环境要求

- Python 3.8+
- Tushare Pro账号（需要积分权限）

### 安装

```bash
# 克隆项目
git clone <repository-url>
cd stock

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入你的 Tushare token
```

### 基本使用

#### 1. 获取基础数据

```bash
# 获取股票基础信息
python scripts/data_fetching/fetch_basic_data.py

# 获取日线数据
python scripts/data_fetching/fetch_daily_data.py --start-date 20150101

# 获取ST状态数据（完整流程）
python scripts/data_fetching/fetch_st_status.py
python scripts/data_fetching/fetch_namechange_history.py
python scripts/data_fetching/fetch_and_merge_st_data.py
```

#### 2. 构建矩阵

```bash
# 构建所有矩阵
python scripts/matrix_building/build_all_matrices.py

# 或选择性构建
python scripts/matrix_building/build_all_matrices.py --matrices st tradability
```

#### 3. 因子分析

```bash
# PB因子分析
python scripts/factor_analysis/analyze_pb_factor_v2.py

# 市值因子分析
python scripts/factor_analysis/analyze_mv_factor_v2.py
```

## 项目结构

```
stock/
├── config/              # 配置文件
├── data_engine/         # 数据引擎
│   ├── api/            # API适配层
│   ├── fetchers/       # 数据获取器
│   ├── processors/     # 矩阵处理器
│   └── utils/          # 工具函数
├── factor_engine/       # 因子引擎
│   └── backtest/       # 回测模块
│       ├── grouping.py      # 分组
│       ├── weighting.py     # 加权
│       ├── metrics.py       # 指标计算
│       ├── visualization.py # 可视化
│       └── export.py        # 导出
└── scripts/            # 脚本层
    ├── data_fetching/  # 数据获取脚本
    ├── matrix_building/# 矩阵构建脚本
    └── factor_analysis/# 因子分析脚本
```

## 核心功能

### 数据引擎（data_engine）

- **API适配**：封装Tushare API，提供统一接口
- **数据获取**：9个Fetcher类，统一继承BaseFetcher
- **矩阵构建**：高效的矩阵化数据处理
- **交易可用性**：综合判断股票是否可交易

### 因子引擎（factor_engine）

- **单因子分析**：完整的因子分析流程
- **风险指标**：12个指标（收益、风险、IC等）
- **可视化**：交互式HTML图表
- **多格式导出**：CSV、Excel、JSON、Parquet

## 数据说明

### 数据目录

默认数据目录：`/Users/cuicui/Documents/stockdata/`

```
stockdata/
├── basic/              # 基础数据
├── daily/              # 日线数据（按股票）
├── supplementary/      # 补充数据（ST、停牌等）
├── matrices/           # 矩阵数据
└── logs/               # 日志
```

### ST数据特殊说明

ST数据通过两个数据源融合：
- **stock_st API**：2016年后的官方数据
- **namechange提取**：从名称变更历史提取，覆盖全时段

最终 `st_status.csv` 包含完整历史数据。

## 风险指标

系统支持12个绩效和风险指标：

**收益指标**：
- 日均收益
- 累计收益
- 年化收益

**风险指标**：
- 收益波动率
- 夏普比率
- 最大回撤
- 卡尔玛比率
- VaR（风险价值）
- CVaR（条件风险价值）
- 下行偏差
- 索提诺比率
- 欧米伽比率

**因子指标**：
- IC（信息系数）
- IR（信息比率）
- IC胜率

## 注意事项

1. **Tushare权限**：部分接口需要足够的积分权限
2. **数据更新**：建议定期更新基础数据和补充数据
3. **存储空间**：完整数据约需10GB空间
4. **计算资源**：矩阵构建和因子分析需要一定计算资源

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request。

## 联系方式

如有问题，请提交Issue。
