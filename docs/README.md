# A股量化投资系统

> 完整的数据获取、矩阵构建、因子分析一体化平台

## 项目特点

- 📊 **双引擎架构**：数据引擎 + 因子引擎
- 🔄 **矩阵化计算**：高效的矩阵化数据处理
- 📈 **37个因子**：覆盖财务、量价、概念、行业多维度
- 🎯 **PIT正确**：真实的点-in-time数据处理
- 🔧 **可扩展**：基类架构，快速添加新因子

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置Tushare Token
cp .env.example .env
# 编辑 .env 填入你的 TUSHARE_TOKEN

# 3. 获取数据
python scripts/data_fetching/fetch_basic_data.py
python scripts/data_fetching/fetch_daily_data.py

# 4. 构建矩阵
python scripts/matrix_building/build_all_matrices.py

# 5. 因子分析
python scripts/factor_analysis/analyze_net_profit_yoy_factor.py
```

## 文档导航

| 文档 | 内容 |
|-----|------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 系统架构、设计决策、目录结构 |
| [FACTORS.md](FACTORS.md) | 37个因子的详细说明和回测表现 |
| [GUIDE.md](GUIDE.md) | 详细使用指南、API文档、常见问题 |
| [ANALYSIS.md](ANALYSIS.md) | 因子分析方案、时间线对齐、避免未来数据 |
| [REFACTORING.md](REFACTORING.md) | 代码重构记录、新框架使用指南 |

## 核心流程

```
数据获取 → 矩阵构建 → 因子分析 → 可视化
    ↓           ↓           ↓
 fetch_*   build_*    analyze_*
```

## 项目结构

```
stock/
├── config/              # 配置
├── data_engine/         # 数据引擎
│   ├── fetchers/       # 数据获取
│   ├── processors/     # 矩阵处理
│   └── utils/          # 工具函数
├── factor_engine/       # 因子引擎
│   └── backtest/       # 回测分析
├── scripts/            # 脚本层
│   ├── data_fetching/
│   ├── matrix_building/
│   └── factor_analysis/
└── docs/               # 文档（本目录）
```

## 核心原则

> **因子值在T日收盘后计算（含T日数据），用于T+1日开盘选股**

详见 [ANALYSIS.md](ANALYSIS.md) 的完整时间线说明。

## 最近更新

- 2026-04-26: 完成代码重构，提取基类和统一接口
- 2026-04-26: 升级pandas至2.0.3，修复依赖
- 2026-04-01: 新增财务报表PIT矩阵体系
- 2026-03-29: 项目结构优化，统一Fetcher继承

---

*更多信息请查看 [GUIDE.md](GUIDE.md) 完整指南*
