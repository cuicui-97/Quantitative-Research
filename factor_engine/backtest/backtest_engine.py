#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基于 FactorMatrix 的回测引擎

核心设计：
- 输入：FactorMatrix 对象（内部是 DataFrame）
- 处理：矩阵对齐 → 股票池筛选 → 分组 → 收益计算
- 输出：回测结果（分组收益、风险指标、IC等、可视化图表）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from data_engine.core.factor_matrix import FactorMatrix

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """回测配置"""
    n_groups: int = 10                    # 分组数量
    enable_cost: bool = False             # 是否计算交易成本
    commission_rate: float = 0.0003       # 佣金率（万3）
    stamp_duty_rate: float = 0.001        # 印花税率（千1）
    slippage_rate: float = 0.001          # 滑点率（千1）
    long_short: bool = True               # 是否计算多空组合

    def __repr__(self) -> str:
        return f"BacktestConfig(n_groups={self.n_groups}, cost={'on' if self.enable_cost else 'off'})"


@dataclass
class BacktestResult:
    """回测结果"""
    config: BacktestConfig
    factor_name: str = ""                 # 因子名称

    # 分组收益 (dates × n_groups)
    group_returns_equal: Optional[pd.DataFrame] = None
    group_returns_mv: Optional[pd.DataFrame] = None

    # 累计收益
    cumulative_equal: Optional[pd.DataFrame] = None
    cumulative_mv: Optional[pd.DataFrame] = None

    # 风险指标 (n_groups × metrics)
    stats_equal: Optional[pd.DataFrame] = None
    stats_mv: Optional[pd.DataFrame] = None

    # IC 分析
    ic_series: Optional[pd.Series] = None
    ic_mean: float = 0.0
    ic_std: float = 0.0
    ic_ir: float = 0.0
    ic_win_rate: float = 0.0

    # 基本信息
    n_dates: int = 0
    n_stocks: int = 0

    def summary(self) -> str:
        """生成回测摘要"""
        lines = [
            "=" * 60,
            f"回测结果摘要: {self.factor_name}",
            "=" * 60,
            f"配置: {self.config}",
            f"数据: {self.n_dates} 个交易日 × {self.n_stocks} 只股票",
            "",
            "IC 分析:",
            f"  IC 均值: {self.ic_mean:.4f}",
            f"  IC 标准差: {self.ic_std:.4f}",
            f"  IC_IR: {self.ic_ir:.2f}",
            f"  IC 胜率: {self.ic_win_rate:.1%}",
        ]

        if self.stats_equal is not None and 'Long-Short' in self.stats_equal.index:
            lines.extend(["", "等权 Long-Short 表现:"])
            ls_stats = self.stats_equal.loc['Long-Short']
            lines.extend([
                f"  年化收益: {ls_stats['annual_return']:.2f}%",
                f"  夏普比率: {ls_stats['sharpe']:.2f}",
                f"  最大回撤: {ls_stats['max_drawdown']:.2f}%",
            ])

        lines.append("=" * 60)
        return "\n".join(lines)

    def save(self, output_dir: Path):
        """保存结果到目录"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 保存分组收益
        if self.group_returns_equal is not None:
            self.group_returns_equal.to_csv(output_dir / f'{self.factor_name}_group_returns_equal.csv')
        if self.group_returns_mv is not None:
            self.group_returns_mv.to_csv(output_dir / f'{self.factor_name}_group_returns_mv.csv')

        # 保存统计指标
        if self.stats_equal is not None:
            self.stats_equal.to_csv(output_dir / f'{self.factor_name}_stats_equal.csv')
        if self.stats_mv is not None:
            self.stats_mv.to_csv(output_dir / f'{self.factor_name}_stats_mv.csv')

        # 保存 IC 序列
        if self.ic_series is not None:
            self.ic_series.to_csv(output_dir / f'{self.factor_name}_ic_series.csv')

        logger.info(f"结果已保存到: {output_dir}")

    def plot_cumulative_returns(self, output_file: Optional[Path] = None):
        """绘制交互式累计收益曲线（HTML）"""
        if self.cumulative_equal is None:
            logger.warning("没有累计收益数据")
            return

        cumulative = self.cumulative_equal

        # 创建子图：等权和市值加权
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('等权分组累计收益', '市值加权分组累计收益'),
            vertical_spacing=0.1
        )

        # 颜色方案：从红到绿（G1差到G10好）
        n_groups = len([c for c in cumulative.columns if c != 'Long-Short'])
        colors = ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf',
                  '#d9ef8b', '#a6d96a', '#66bd63', '#1a9850', '#006837']

        # 绘制等权收益
        for i, col in enumerate(cumulative.columns):
            if col == 'Long-Short':
                # 多空组合用黑色虚线
                fig.add_trace(
                    go.Scatter(
                        x=cumulative.index,
                        y=cumulative[col],
                        name=f'{col} (等权)',
                        line=dict(color='black', width=3, dash='dash'),
                        hovertemplate='%{x}<br>%{y:.2%}<extra></extra>'
                    ),
                    row=1, col=1
                )
            else:
                color_idx = int(col.replace('G', '')) - 1 if col.startswith('G') else i
                fig.add_trace(
                    go.Scatter(
                        x=cumulative.index,
                        y=cumulative[col],
                        name=f'{col}',
                        line=dict(color=colors[color_idx], width=1.5),
                        hovertemplate='%{x}<br>%{y:.2%}<extra></extra>',
                        showlegend=True if i < n_groups else False
                    ),
                    row=1, col=1
                )

        # 绘制市值加权收益（如果有）
        if self.cumulative_mv is not None:
            for i, col in enumerate(self.cumulative_mv.columns):
                if col == 'Long-Short':
                    fig.add_trace(
                        go.Scatter(
                            x=self.cumulative_mv.index,
                            y=self.cumulative_mv[col],
                            name=f'{col} (市值加权)',
                            line=dict(color='black', width=3, dash='dash'),
                            hovertemplate='%{x}<br>%{y:.2%}<extra></extra>'
                        ),
                        row=2, col=1
                    )
                else:
                    color_idx = int(col.replace('G', '')) - 1 if col.startswith('G') else i
                    fig.add_trace(
                        go.Scatter(
                            x=self.cumulative_mv.index,
                            y=self.cumulative_mv[col],
                            name=f'{col}',
                            line=dict(color=colors[color_idx], width=1.5),
                            hovertemplate='%{x}<br>%{y:.2%}<extra></extra>',
                            showlegend=False
                        ),
                        row=2, col=1
                    )

        # 更新布局
        fig.update_layout(
            title=f'{self.factor_name} 分组累计收益',
            height=800,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.2,
                xanchor="center",
                x=0.5
            )
        )

        # Y轴格式化为百分比
        fig.update_yaxes(tickformat='.0%')

        if output_file:
            fig.write_html(output_file)
            logger.info(f"累计收益图已保存: {output_file}")

        return fig

    def plot_group_statistics(self, output_file: Optional[Path] = None):
        """绘制交互式分组统计指标（HTML）"""
        if self.stats_equal is None:
            logger.warning("没有统计数据")
            return

        stats = self.stats_equal

        # 移除多空组合行
        if 'Long-Short' in stats.index:
            stats_groups = stats.iloc[:-1]
            ls_stats = stats.loc[['Long-Short']]
        else:
            stats_groups = stats
            ls_stats = None

        x_labels = list(stats_groups.index)

        # 创建子图
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                '日均收益率 (%)',
                '夏普比率',
                '胜率 (%)',
                '最大回撤 (%)'
            ),
            vertical_spacing=0.15
        )

        colors = ['#67a9cf'] * len(x_labels)

        # 1. 平均收益
        fig.add_trace(
            go.Bar(
                x=x_labels,
                y=stats_groups['mean_return'],
                marker_color=colors,
                hovertemplate='%{x}<br>日均收益: %{y:.3f}%<extra></extra>'
            ),
            row=1, col=1
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red", row=1, col=1)

        # 2. 夏普比率
        fig.add_trace(
            go.Bar(
                x=x_labels,
                y=stats_groups['sharpe'],
                marker_color=colors,
                hovertemplate='%{x}<br>夏普: %{y:.2f}<extra></extra>'
            ),
            row=1, col=2
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red", row=1, col=2)

        # 3. 胜率
        fig.add_trace(
            go.Bar(
                x=x_labels,
                y=stats_groups['win_rate'],
                marker_color=colors,
                hovertemplate='%{x}<br>胜率: %{y:.1f}%<extra></extra>'
            ),
            row=2, col=1
        )
        fig.add_hline(y=50, line_dash="dash", line_color="red", row=2, col=1)

        # 4. 最大回撤
        fig.add_trace(
            go.Bar(
                x=x_labels,
                y=stats_groups['max_drawdown'],
                marker_color=colors,
                hovertemplate='%{x}<br>最大回撤: %{y:.1f}%<extra></extra>'
            ),
            row=2, col=2
        )

        # 如果有Long-Short数据，添加标注
        if ls_stats is not None:
            for i, (idx, row) in enumerate(ls_stats.iterrows()):
                for col_idx, metric in enumerate(['mean_return', 'sharpe', 'win_rate', 'max_drawdown'], 1):
                    row_idx = (col_idx - 1) // 2 + 1
                    col = (col_idx - 1) % 2 + 1
                    # 添加水平参考线显示多空组合值
                    fig.add_hline(
                        y=row[metric],
                        line_dash="dot",
                        line_color="green",
                        annotation_text=f"L-S: {row[metric]:.2f}",
                        row=row_idx, col=col
                    )

        # 更新布局
        fig.update_layout(
            title=f'{self.factor_name} 分组风险指标',
            height=700,
            showlegend=False
        )

        if output_file:
            fig.write_html(output_file)
            logger.info(f"统计图已保存: {output_file}")

        return fig

        # 3. 胜率
        ax = axes[1, 0]
        bars = ax.bar(range(len(stats_groups)), stats_groups['win_rate'],
                     color='lightgreen', alpha=0.7)
        ax.axhline(y=50, color='red', linestyle='--', linewidth=1)
        ax.set_title('Win Rate by Group (%)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Group', fontsize=10)
        ax.set_ylabel('Win Rate (%)', fontsize=10)
        ax.set_xticks(range(len(stats_groups)))
        ax.set_xticklabels(stats_groups.index)
        ax.set_ylim([0, 100])
        ax.grid(True, alpha=0.3, axis='y')

        # 4. 累计收益
        ax = axes[1, 1]
        bars = ax.bar(range(len(stats_groups)), stats_groups['cumulative_return'],
                     color='gold', alpha=0.7)
        ax.axhline(y=0, color='red', linestyle='--', linewidth=1)
        ax.set_title('Cumulative Return by Group (%)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Group', fontsize=10)
        ax.set_ylabel('Return (%)', fontsize=10)
        ax.set_xticks(range(len(stats_groups)))
        ax.set_xticklabels(stats_groups.index)
        ax.grid(True, alpha=0.3, axis='y')

        plt.suptitle(f'{self.factor_name}: Performance Statistics by Group',
                    fontsize=14, fontweight='bold', y=1.02)
        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            logger.info(f"统计图已保存: {output_file}")

        plt.close()

    def generate_report(self, output_dir: Path):
        """生成完整报告（数据+交互式HTML图表）"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 保存数据
        self.save(output_dir)

        # 生成交互式HTML图表
        self.plot_cumulative_returns(output_dir / f'{self.factor_name}_cumulative_returns.html')
        self.plot_group_statistics(output_dir / f'{self.factor_name}_statistics.html')

        logger.info(f"完整报告已生成: {output_dir}")


class BacktestEngine:
    """
    回测引擎

    使用示例:
        >>> engine = BacktestEngine(config=BacktestConfig(n_groups=10))
        >>>
        >>> # 加载矩阵
        >>> engine.load_factor(factor_matrix)
        >>> engine.load_returns(returns_matrix)
        >>> engine.load_tradable(tradable_matrix)
        >>> engine.load_mv(mv_matrix)
        >>> engine.load_index_component(zz500_matrix, name='zz500')
        >>>
        >>> # 运行回测
        >>> result = engine.run()
        >>>
        >>> # 生成完整报告（数据+图表）
        >>> result.generate_report(output_dir)
    """

    def __init__(self, config: Optional[BacktestConfig] = None):
        self.config = config or BacktestConfig()
        self.logger = logging.getLogger(self.__class__.__name__)

        # 矩阵存储
        self._factor: Optional[FactorMatrix] = None
        self._returns: Optional[FactorMatrix] = None
        self._tradable: Optional[FactorMatrix] = None
        self._mv: Optional[FactorMatrix] = None
        self._index_component: Optional[FactorMatrix] = None

        # 对齐后的矩阵
        self._aligned: Optional[Dict[str, FactorMatrix]] = None

    # ==================== 加载方法 ====================

    def load_factor(self, matrix: FactorMatrix) -> BacktestEngine:
        """加载因子矩阵"""
        self._factor = matrix
        self.logger.info(f"加载因子: {matrix.name}")
        return self

    def load_returns(self, matrix: FactorMatrix) -> BacktestEngine:
        """加载收益矩阵（T日→T+1日开盘收益）"""
        self._returns = matrix
        self.logger.info(f"加载收益: {matrix.name}")
        return self

    def load_tradable(self, matrix: FactorMatrix) -> BacktestEngine:
        """
        加载可交易性矩阵

        约定: 0=可交易, 1=不可交易 (与原来一致)
        """
        self._tradable = matrix
        self.logger.info(f"加载可交易性: {matrix.name}")
        return self

    def load_mv(self, matrix: FactorMatrix) -> BacktestEngine:
        """加载市值矩阵（用于加权）"""
        self._mv = matrix
        self.logger.info(f"加载市值: {matrix.name}")
        return self

    def load_index_component(self, matrix: FactorMatrix, name: str = "index") -> BacktestEngine:
        """
        加载指数成分股矩阵

        用于限定股票池（如沪深300、中证500等）
        约定: 1=是成分股, 0=不是
        """
        self._index_component = matrix
        self._index_component.name = name
        self.logger.info(f"加载指数成分: {name}")
        return self

    # ==================== 核心流程 ====================

    def run(self) -> BacktestResult:
        """
        运行完整回测流程

        流程:
        1. 矩阵对齐
        2. 股票池筛选
        3. 因子分组
        4. 收益计算（等权 + 市值加权）
        5. 风险指标计算
        6. IC分析
        """
        self.logger.info("=" * 60)
        self.logger.info("开始回测")
        self.logger.info("=" * 60)
        self.logger.info(f"配置: {self.config}")

        # 1. 检查必需矩阵
        if self._factor is None:
            raise ValueError("必须先加载因子矩阵")
        if self._returns is None:
            raise ValueError("必须先加载收益矩阵")

        factor_name = self._factor.name

        # 2. 矩阵对齐
        self.logger.info("\n[1/5] 矩阵对齐...")
        self._align_matrices()

        # 3. 股票池筛选
        self.logger.info("\n[2/5] 股票池筛选...")
        self._apply_stock_filter()

        # 4. 因子分组
        self.logger.info(f"\n[3/5] 因子分组 (n={self.config.n_groups})...")
        group_matrix = self._group_factor()

        # 5. 计算收益、换手率和持仓数量
        self.logger.info("\n[4/5] 计算分组收益、换手率和持仓数量...")
        returns_equal, returns_mv, turnover_equal, turnover_mv, position_count = self._calculate_group_returns_and_turnover(group_matrix)

        # 6. 计算风险指标（包含换手率和持仓数量）
        self.logger.info("\n[5/5] 计算风险指标...")
        stats_equal = self._calculate_statistics(returns_equal, turnover_equal, position_count)
        stats_mv = self._calculate_statistics(returns_mv, turnover_mv) if returns_mv is not None else None

        # 7. IC分析
        self.logger.info("\n[6/6] IC分析...")
        ic_series = self._calculate_ic()

        # 构建结果
        result = BacktestResult(
            config=self.config,
            factor_name=factor_name,
            group_returns_equal=returns_equal,
            group_returns_mv=returns_mv,
            cumulative_equal=self._calculate_cumulative(returns_equal),
            cumulative_mv=self._calculate_cumulative(returns_mv) if returns_mv is not None else None,
            stats_equal=stats_equal,
            stats_mv=stats_mv,
            ic_series=ic_series,
            ic_mean=ic_series.mean() if ic_series is not None else 0.0,
            ic_std=ic_series.std() if ic_series is not None else 0.0,
            ic_ir=ic_series.mean() / ic_series.std() if ic_series is not None and ic_series.std() > 0 else 0.0,
            ic_win_rate=(ic_series > 0).mean() if ic_series is not None else 0.0,
            n_dates=self._aligned['factor'].shape[0],
            n_stocks=self._aligned['factor'].shape[1]
        )

        self.logger.info("\n" + result.summary())

        return result

    # ==================== 内部方法 ====================

    def _align_matrices(self):
        """对齐所有矩阵到共同维度"""
        matrices = {
            'factor': self._factor,
            'returns': self._returns,
        }

        if self._tradable is not None:
            matrices['tradable'] = self._tradable
        if self._mv is not None:
            matrices['mv'] = self._mv
        if self._index_component is not None:
            matrices['index'] = self._index_component

        # 计算所有矩阵的交集
        common_index = matrices['factor'].index
        common_columns = matrices['factor'].columns

        for name, matrix in matrices.items():
            if name == 'factor':
                continue
            common_index = common_index.intersection(matrix.index)
            common_columns = common_columns.intersection(matrix.columns)

        self.logger.info(f"  共同维度: {len(common_index)} 天 × {len(common_columns)} 股票")

        # 一次性对齐所有矩阵
        aligned = {}
        for name, matrix in matrices.items():
            aligned[name] = matrix.slice(common_index, common_columns)

        self._aligned = aligned
        self.logger.info(f"  对齐后: {len(common_index)} 天 × {len(common_columns)} 股票")

    def _apply_stock_filter(self):
        """应用股票池筛选"""
        factor = self._aligned['factor']

        # 构建综合可交易性矩阵
        # 原则：只要有一个条件不满足，就不可交易
        tradable_mask = pd.DataFrame(True, index=factor.index, columns=factor.columns)

        # 基础可交易性
        if 'tradable' in self._aligned:
            # 约定：0=可交易，1=不可交易
            tradable_mask &= (self._aligned['tradable'].data == 0)
            valid_count = (self._aligned['tradable'].data == 0).sum().sum()
            self.logger.info(f"  可交易筛选: {valid_count:,} 个有效")

        # 指数成分股筛选
        if 'index' in self._aligned:
            # 约定：1=是成分股
            tradable_mask &= (self._aligned['index'].data == 1)
            valid_count = (self._aligned['index'].data == 1).sum().sum()
            self.logger.info(f"  指数成分筛选: {valid_count:,} 个有效")

        # 应用到因子矩阵（不可交易的位置设为 NaN）
        factor_data = factor.data.where(tradable_mask)

        self._aligned['factor'] = FactorMatrix(
            name=factor.name,
            data=factor_data,
            description=factor.description
        )

    def _group_factor(self) -> pd.DataFrame:
        """
        对因子进行分组（截面分组）

        每天独立对所有股票按因子值排序，分成 n_groups 组
        使用等宽分箱：每组因子值区间相同（不考虑股票数量是否相等）

        返回: 分组矩阵 DataFrame (dates × stocks)
               值为 1~n_groups（第1组因子值最小，第n_groups组最大）
               NaN 表示该股票当日因子缺失或不可交易
        """
        factor_data = self._aligned['factor'].data
        n_groups = self.config.n_groups

        # 步骤1: 截面排名（每天独立）
        # rank(pct=True) 生成百分位排名 (0.0 ~ 1.0)
        # axis=1 表示按行（每天内对所有股票排名）
        # na_option='keep' 表示空值保持 NaN（默认行为）
        group_data = factor_data.rank(axis=1, pct=True)

        # 步骤2: 百分位切分组
        # pd.cut 把 0.0~1.0 切成 n_groups 个等宽区间
        # bins=n_groups: 切10组 → 区间 [0.0-0.1), [0.1-0.2), ..., [0.9-1.0]
        # labels=range(1, n_groups+1): 标记为 1, 2, ..., 10
        # NaN 会保持 NaN（pd.cut 默认行为）
        group_data = group_data.apply(lambda x: pd.cut(x, bins=n_groups, labels=range(1, n_groups + 1)), axis=1)

        # 步骤3: 类型转换
        # 从 Categorical 转为 float32，方便后续计算
        group_data = group_data.astype(np.float32)

        # 统计信息：日均有效样本数（非NaN）
        self.logger.info(f"  分组完成，日均样本数: {group_data.count(axis=1).mean():.0f}")

        return group_data

    def _calculate_group_returns_and_turnover(self, group_matrix: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], pd.DataFrame, Optional[pd.DataFrame], pd.DataFrame]:
        """
        计算分组收益、换手率和持仓数量（向量化版本）

        Returns:
            (等权收益 DataFrame, 市值加权收益 DataFrame, 等权换手率 DataFrame, 市值加权换手率 DataFrame, 持仓数量 DataFrame)
        """
        returns_data = self._aligned['returns'].data
        n_groups = self.config.n_groups

        # 对齐分组和收益：
        returns_shifted = returns_data.shift(-1)

        # 计算收益
        group_returns_equal = {}
        group_returns_mv = None

        for g in range(1, n_groups + 1):
            masked_returns = returns_shifted.where(group_matrix == g)
            group_returns_equal[f"G{g}"] = masked_returns.mean(axis=1)

        if 'mv' in self._aligned:
            mv_data = self._aligned['mv'].data
            group_returns_mv = {}
            for g in range(1, n_groups + 1):
                mask = (group_matrix == g)
                masked_mv = mv_data.where(mask, 0)
                weights = masked_mv.div(masked_mv.sum(axis=1), axis=0)
                weighted_returns = (returns_shifted * weights).sum(axis=1)
                group_returns_mv[f"G{g}"] = weighted_returns

        # 转为 DataFrame（去掉最后一行）
        df_equal = pd.DataFrame(group_returns_equal).iloc[:-1]
        df_mv = pd.DataFrame(group_returns_mv).iloc[:-1] if group_returns_mv else None

        # 计算持仓数量（每日每组有多少只股票）
        position_count = {}
        for g in range(1, n_groups + 1):
            mask = (group_matrix == g)
            count = mask.sum(axis=1)  # 每行求和 = 该日该组股票数
            position_count[f"G{g}"] = count

        df_position_count = pd.DataFrame(position_count).iloc[:-1]  # 与收益对齐

        # 计算换手率：基于分组持仓变化
        turnover_equal = {}

        for g in range(1, n_groups + 1):
            # 创建布尔掩码
            mask_today = (group_matrix == g).astype(bool)
            # 昨日持仓
            mask_yesterday = mask_today.shift(1).fillna(False).astype(bool)

            # 重叠股票数
            overlap = (mask_yesterday & mask_today).sum(axis=1)
            # 昨日持仓数
            count_yesterday = mask_yesterday.sum(axis=1)
            # 换手率 = 1 - 重叠比例（避免除0）
            turnover = pd.Series(np.nan, index=group_matrix.index)
            valid = count_yesterday > 0
            turnover[valid] = 1 - (overlap[valid] / count_yesterday[valid])
            turnover_equal[f"G{g}"] = turnover

        df_turnover_equal = pd.DataFrame(turnover_equal)
        # 去掉第一行（无昨日数据）和最后一行（shift问题）
        df_turnover_equal = df_turnover_equal.iloc[1:-1]

        # 添加 Long-Short 组合
        if self.config.long_short:
            df_equal["Long-Short"] = df_equal.iloc[:, -1] - df_equal.iloc[:, 0]
            df_turnover_equal["Long-Short"] = df_turnover_equal.iloc[:, :].mean(axis=1)  # L-S换手率取平均
            # 持仓数量：多空组合不直接适用，取平均
            df_position_count["Long-Short"] = df_position_count.iloc[:, :].mean(axis=1)
            if df_mv is not None:
                df_mv["Long-Short"] = df_mv.iloc[:, -1] - df_mv.iloc[:, 0]

        self.logger.info(f"  等权收益计算完成: {df_equal.shape}")
        self.logger.info(f"  换手率计算完成: {df_turnover_equal.shape}")
        self.logger.info(f"  持仓数量计算完成: {df_position_count.shape}")
        if df_mv is not None:
            self.logger.info(f"  市值加权收益计算完成: {df_mv.shape}")

        return df_equal, df_mv, df_turnover_equal, None, df_position_count

    def _calculate_statistics(self, returns_df: pd.DataFrame, turnover_df: Optional[pd.DataFrame] = None, position_count_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        计算风险指标（向量化版本）

        对整个 DataFrame 进行矩阵运算，避免逐列循环
        """
        # 基础统计（自动忽略NaN）
        mean = returns_df.mean() * 100  # 日均收益率（%）
        std = returns_df.std() * 100    # 标准差（%）

        # 累计收益：(1+r).prod() - 1
        cumulative = ((1 + returns_df).prod() - 1) * 100  # (%)

        # 年化收益
        n_days = returns_df.count()
        annual_return = ((1 + cumulative/100) ** (252 / n_days) - 1) * 100

        # 夏普比率
        sharpe = (mean / std) * np.sqrt(252)
        sharpe = sharpe.replace([np.inf, -np.inf], 0)

        # 胜率
        win_rate = (returns_df > 0).mean() * 100

        # 最大回撤（向量化计算）
        # cumprod: 累计收益曲线
        cum_returns = (1 + returns_df).cumprod()
        # expanding().max(): 历史最大值
        rolling_max = cum_returns.expanding().max()
        # 计算回撤
        drawdown = (cum_returns - rolling_max) / rolling_max * 100
        max_drawdown = drawdown.min()

        # 组装结果
        stats = pd.DataFrame({
            'mean_return': mean,
            'std': std,
            'sharpe': sharpe,
            'win_rate': win_rate,
            'cumulative_return': cumulative,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown
        })

        # 添加换手率（如果提供）
        if turnover_df is not None:
            # turnover_df 的列名是 G1, G2...，需要对齐到 stats 的索引
            # 计算每列的平均换手率，然后按列名对齐
            turnover_mean = turnover_df.mean() * 100  # 转为百分比
            # 确保 turnover_mean 的索引与 stats 的索引匹配
            stats['turnover'] = [turnover_mean.get(idx, 0) for idx in stats.index]

        # 添加平均持仓数量（如果提供）
        if position_count_df is not None:
            # 计算每列的平均持仓数量
            position_mean = position_count_df.mean()
            stats['avg_position_count'] = [position_mean.get(idx, 0) for idx in stats.index]

        # 处理全NaN列（用0填充）
        stats = stats.fillna(0)

        return stats

    def _calculate_cumulative(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算累计收益"""
        return (1 + returns_df).cumprod() - 1

    def _calculate_ic(self) -> Optional[pd.Series]:
        """
        计算 IC (Information Coefficient) - 向量化版本

        IC = Rank(因子值) 与 Rank(T+1收益) 的相关系数
        """
        factor_data = self._aligned['factor'].data
        returns_data = self._aligned['returns'].data

        # 对齐：T日因子对应 T+1日收益
        returns_shifted = returns_data.shift(-1)

        # 截面排名
        factor_rank = factor_data.rank(axis=1, pct=False, na_option='keep')  # 使用原始排名
        returns_rank = returns_shifted.rank(axis=1, pct=False, na_option='keep')

        # 使用 corrwith 计算每行相关系数（更可靠）
        # 但需要处理NaN，所以用逐行的方式
        min_samples = 10
        ics = []
        dates = []

        for date in factor_rank.index[:-1]:  # 去掉最后一行（shift导致的NaN）
            f_row = factor_rank.loc[date]
            r_row = returns_rank.loc[date]

            # 两者都有效
            valid = f_row.notna() & r_row.notna()
            if valid.sum() < min_samples:
                continue

            # 计算秩相关系数（Spearman）
            from scipy.stats import spearmanr
            ic, _ = spearmanr(f_row[valid], r_row[valid])
            if not pd.isna(ic):
                ics.append(ic)
                dates.append(date)

        return pd.Series(ics, index=dates)
