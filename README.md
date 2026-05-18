# A股量化投资系统

> 完整的数据获取、矩阵构建、因子分析一体化平台

## 项目特点

- 📊 **双引擎架构**：数据引擎 + 因子引擎
- 🔄 **矩阵化计算**：高效的矩阵化数据处理
- 📈 **37个因子**：覆盖财务、量价、概念、行业多维度
- 🎯 **PIT正确**：真实的点-in-time数据处理

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置Tushare Token
cp .env.example .env
# 编辑 .env 填入 TUSHARE_TOKEN

# 3. 获取数据
python scripts/data_fetching/fetch_basic_data.py
python scripts/data_fetching/fetch_daily_data.py

# 4. 构建矩阵
python scripts/matrix_building/build_all_matrices.py

# 5. 因子分析
python scripts/factor_analysis/analyze_net_profit_yoy_factor.py
```

## 文档

详细文档请查看 [docs/](./docs/) 目录：

| 文档 | 内容 |
|-----|------|
| [docs/README.md](docs/README.md) | 项目概览 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 系统架构、设计决策 |
| [docs/FACTORS.md](docs/FACTORS.md) | 37个因子的详细说明 |
| [docs/GUIDE.md](docs/GUIDE.md) | 使用指南、API文档 |
| [docs/ANALYSIS.md](docs/ANALYSIS.md) | 因子分析方案、时间线对齐 |
| [docs/REFACTORING.md](docs/REFACTORING.md) | 代码重构记录 |

## 核心原则

> **因子值在T日收盘后计算（含T日数据），用于T+1日开盘选股**

详见 [docs/ANALYSIS.md](docs/ANALYSIS.md) 的完整时间线说明。

## 项目结构

```
stock/
├── config/              # 配置
├── data_engine/         # 数据引擎
├── factor_engine/       # 因子引擎
├── scripts/            # 脚本层
│   ├── data_fetching/  # 数据获取
│   ├── matrix_building/# 矩阵构建
│   └── factor_analysis/# 因子分析
└── docs/               # 文档目录
```

## 最近更新

- 2026-04-26: 完成代码重构，提取基类和统一接口
- 2026-04-26: 整理文档，统一放到docs目录
- 2026-04-01: 新增财务报表PIT矩阵体系

---

*详细使用说明请查看 [docs/GUIDE.md](docs/GUIDE.md)*
