# 项目结构优化记录

## 2026-03-28: 脚本分类重组

### 变更说明

将 `scripts/` 目录下的所有脚本按功能分类，提升项目可维护性和可读性。

### 优化前结构

```
scripts/
├── analyze_mv_factor.py
├── analyze_pb_factor.py
├── analyze_tradability_matrix.py
├── build_all_matrices.py
├── build_price_matrices.py
├── build_trade_calendar.py
├── build_valuation_matrices.py
├── check_api_health.py
├── fetch_basic_data.py
├── fetch_daily_data.py
├── fetch_fama_data.py
├── fetch_limit_prices.py
├── fetch_st_status.py
├── fetch_suspension_status.py
└── validate_fama_data.py
```

**问题**:
- 所有脚本混在一起，不易查找
- 功能边界不清晰
- 新人难以理解项目流程

### 优化后结构

```
scripts/
├── README.md                          # 脚本总览和使用指南
├── data_fetching/                     # 数据获取相关
│   ├── README.md                      # 详细说明文档
│   ├── check_api_health.py            # API健康检查
│   ├── fetch_basic_data.py            # 获取基础数据
│   ├── fetch_daily_data.py            # 获取日线数据
│   ├── fetch_st_status.py             # 获取ST状态
│   ├── fetch_suspension_status.py     # 获取停牌信息
│   ├── fetch_limit_prices.py          # 获取涨跌停价格
│   ├── fetch_fama_data.py             # 获取Fama因子
│   └── validate_fama_data.py          # 验证Fama数据
├── matrix_building/                   # 矩阵构建相关
│   ├── README.md                      # 详细说明文档
│   ├── build_all_matrices.py          # 一键构建所有矩阵
│   ├── build_trade_calendar.py        # 构建交易日历
│   ├── build_price_matrices.py        # 构建价格矩阵
│   └── build_valuation_matrices.py    # 构建估值矩阵
└── factor_analysis/                   # 因子分析相关
    ├── README.md                      # 详细说明文档
    ├── analyze_pb_factor.py           # PB因子分析
    ├── analyze_mv_factor.py           # 市值因子分析
    └── analyze_tradability_matrix.py  # 可交易性分析
```

### 改进点

1. **清晰的功能分类**
   - 数据获取（data_fetching）
   - 矩阵构建（matrix_building）
   - 因子分析（factor_analysis）

2. **完善的文档体系**
   - 每个子目录都有独立的README
   - 脚本总览文档（scripts/README.md）
   - 详细的使用说明和参数说明

3. **更好的可维护性**
   - 按功能模块组织，便于查找
   - 依赖关系清晰（数据获取 → 矩阵构建 → 因子分析）
   - 新增脚本时知道放在哪个目录

4. **改进的用户体验**
   - 新人可以按目录顺序学习
   - 每个目录有独立的使用指南
   - 明确的执行顺序建议

### 文件移动记录

#### data_fetching/
- `check_api_health.py`
- `fetch_basic_data.py`
- `fetch_daily_data.py`
- `fetch_st_status.py`
- `fetch_suspension_status.py`
- `fetch_limit_prices.py`
- `fetch_fama_data.py`
- `validate_fama_data.py`

#### matrix_building/
- `build_all_matrices.py`
- `build_trade_calendar.py`
- `build_price_matrices.py`
- `build_valuation_matrices.py`

#### factor_analysis/
- `analyze_pb_factor.py`
- `analyze_mv_factor.py`
- `analyze_tradability_matrix.py`

### 兼容性说明

**重要**: 脚本内部的导入路径**无需修改**，因为：
1. 所有脚本都使用绝对导入（从项目根目录开始）
2. Python模块搜索路径包含项目根目录

**执行方式变更**:

```bash
# 旧方式（仍然有效，但不推荐）
python scripts/fetch_basic_data.py

# 新方式（推荐）
python scripts/data_fetching/fetch_basic_data.py
```

### 后续工作

- [x] 重组脚本目录结构
- [x] 创建各子目录的README文档
- [x] 更新主README文档
- [x] 优化因子分析结果文件夹结构（按因子分类）
- [ ] 更新PROJECT.md文档
- [ ] 添加快速入门教程
- [ ] 创建示例Jupyter Notebook

### 相关文档

- [scripts/README.md](scripts/README.md) - 脚本总览
- [scripts/data_fetching/README.md](scripts/data_fetching/README.md) - 数据获取详解
- [scripts/matrix_building/README.md](scripts/matrix_building/README.md) - 矩阵构建详解
- [scripts/factor_analysis/README.md](scripts/factor_analysis/README.md) - 因子分析详解
- [README.md](README.md) - 项目主文档

---

## 其他变更

### 因子分析结果文件夹优化

**优化前**:
```
factor_analysis_results/
├── pb_factor_statistics_equal.csv
├── pb_factor_statistics_mv_weighted.csv
├── pb_factor_cumulative_returns_equal.png
├── ...（共16个文件）
└── README.md
```

**优化后**:
```
factor_analysis_results/
├── README.md
├── pb_factor/                         # PB因子结果
│   ├── pb_factor_statistics_equal.csv
│   ├── pb_factor_statistics_mv_weighted.csv
│   ├── pb_factor_group_returns_equal.csv
│   ├── pb_factor_group_returns_mv_weighted.csv
│   └── 4张图表
└── mv_factor/                         # 市值因子结果
    ├── mv_factor_statistics_equal.csv
    ├── mv_factor_statistics_mv_weighted.csv
    ├── mv_factor_group_returns_equal.csv
    ├── mv_factor_group_returns_mv_weighted.csv
    └── 4张图表
```

**改进**:
- 按因子分类，结构更清晰
- 便于添加新因子的分析结果
- 脚本已更新，自动输出到对应子文件夹
