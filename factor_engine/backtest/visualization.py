"""
可视化模块

负责生成因子分析的图表
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from typing import Optional
import logging

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False


class FactorVisualizer:
    """
    因子可视化工具

    生成因子分析的标准图表：
    1. 累计收益曲线图
    2. 统计指标柱状图
    """

    @staticmethod
    def plot_cumulative_returns(
        group_returns: pd.DataFrame,
        factor_name: str,
        output_file: Optional[str] = None
    ):
        """
        绘制累计收益曲线

        Args:
            group_returns: 分组收益 DataFrame
            factor_name: 因子名称（用于标题）
            output_file: 输出文件路径
        """
        # 计算累计收益
        cumulative = (1 + group_returns).cumprod()

        plt.figure(figsize=(14, 8))

        # 绘制各组累计收益（使用渐变色）
        n_groups = len(group_returns.columns)
        colors = plt.cm.RdYlGn_r(np.linspace(0, 1, n_groups))

        for i, col in enumerate(cumulative.columns):
            plt.plot(
                range(len(cumulative)),
                cumulative[col],
                label=f'Group {col}',
                color=colors[i],
                linewidth=1.5
            )

        # 绘制多空组合（黑色虚线，加粗）
        # 使用实际的列索引，而不是假设的整数值
        first_group = group_returns.columns[0]
        last_group = group_returns.columns[-1]
        long_short_cum = (1 + (group_returns[last_group] - group_returns[first_group])).cumprod()
        plt.plot(
            range(len(long_short_cum)),
            long_short_cum,
            label=f'Long-Short ({last_group}-{first_group})',
            linewidth=2.5,
            linestyle='--',
            color='black'
        )

        plt.xlabel('Trading Days', fontsize=12)
        plt.ylabel('Cumulative Return', fontsize=12)
        plt.title(f'{factor_name} Factor: Cumulative Returns by Decile Group',
                 fontsize=14, fontweight='bold')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"图表已保存: {output_file}")

        plt.close()

    @staticmethod
    def plot_group_statistics(
        stats: pd.DataFrame,
        factor_name: str,
        output_file: Optional[str] = None
    ):
        """
        绘制分组统计指标柱状图

        Args:
            stats: 统计指标 DataFrame
            factor_name: 因子名称（用于标题）
            output_file: 输出文件路径
        """
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # 移除多空组合行（单独处理）
        stats_groups = stats.iloc[:-1]

        # 1. 平均收益
        ax = axes[0, 0]
        bars = ax.bar(range(len(stats_groups)), stats_groups['mean_return'],
                     color='steelblue', alpha=0.7)
        ax.axhline(y=0, color='red', linestyle='--', linewidth=1)
        ax.set_title('Average Daily Return by Group (%)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Group', fontsize=10)
        ax.set_ylabel('Return (%)', fontsize=10)
        ax.set_xticks(range(len(stats_groups)))
        ax.set_xticklabels(stats_groups.index)
        ax.grid(True, alpha=0.3, axis='y')

        # 2. 夏普比率
        ax = axes[0, 1]
        bars = ax.bar(range(len(stats_groups)), stats_groups['sharpe_ratio'],
                     color='coral', alpha=0.7)
        ax.axhline(y=0, color='red', linestyle='--', linewidth=1)
        ax.set_title('Sharpe Ratio by Group', fontsize=12, fontweight='bold')
        ax.set_xlabel('Group', fontsize=10)
        ax.set_ylabel('Sharpe Ratio', fontsize=10)
        ax.set_xticks(range(len(stats_groups)))
        ax.set_xticklabels(stats_groups.index)
        ax.grid(True, alpha=0.3, axis='y')

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
        ax.set_ylabel('Cumulative Return (%)', fontsize=10)
        ax.set_xticks(range(len(stats_groups)))
        ax.set_xticklabels(stats_groups.index)
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"图表已保存: {output_file}")

        plt.close()


# ==================== 增强版可视化函数（Plotly交互式图表） ====================

def plot_combined_returns(
    group_returns_equal: pd.DataFrame,
    group_returns_mv: pd.DataFrame,
    index_returns: dict,
    factor_name: str,
    output_dir: Path,
    start_date: str = '2015-01-01'
):
    """
    绘制合并的累计收益曲线（等权+市值加权+指数基准）

    使用 Plotly 生成交互式 HTML 图表

    Args:
        group_returns_equal: 等权分组收益率
        group_returns_mv: 市值加权分组收益率
        index_returns: 指数收益率字典
        factor_name: 因子名称
        output_dir: 输出目录
        start_date: 起始日期
    """
    logger = logging.getLogger(__name__)
    logger.info(f"绘制合并累计收益曲线（从 {start_date} 开始）...")

    group_returns_equal.index = pd.to_datetime(group_returns_equal.index, format='%Y%m%d')
    group_returns_mv.index = pd.to_datetime(group_returns_mv.index, format='%Y%m%d')

    start = pd.to_datetime(start_date)
    group_returns_equal = group_returns_equal[group_returns_equal.index >= start]
    group_returns_mv = group_returns_mv[group_returns_mv.index >= start]

    cum_returns_equal = (1 + group_returns_equal).cumprod()
    cum_returns_mv = (1 + group_returns_mv).cumprod()

    # 计算 Long-Short 组合收益
    if 10 in group_returns_equal.columns and 1 in group_returns_equal.columns:
        long_short_equal = group_returns_equal[10] - group_returns_equal[1]
        cum_long_short_equal = (1 + long_short_equal).cumprod()
    elif 'Group 10' in group_returns_equal.columns and 'Group 1' in group_returns_equal.columns:
        long_short_equal = group_returns_equal['Group 10'] - group_returns_equal['Group 1']
        cum_long_short_equal = (1 + long_short_equal).cumprod()
    else:
        cum_long_short_equal = None

    if 10 in group_returns_mv.columns and 1 in group_returns_mv.columns:
        long_short_mv = group_returns_mv[10] - group_returns_mv[1]
        cum_long_short_mv = (1 + long_short_mv).cumprod()
    elif 'Group 10' in group_returns_mv.columns and 'Group 1' in group_returns_mv.columns:
        long_short_mv = group_returns_mv['Group 10'] - group_returns_mv['Group 1']
        cum_long_short_mv = (1 + long_short_mv).cumprod()
    else:
        cum_long_short_mv = None

    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(
            f'{factor_name} 因子分组累计收益曲线 - 等权重（{start_date}至今）',
            f'{factor_name} 因子分组累计收益曲线 - 市值加权（{start_date}至今）'
        ),
        vertical_spacing=0.1
    )

    colors = ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#d9ef8b',
              '#a6d96a', '#66bd63', '#1a9850', '#006837', '#004529']

    # 子图1: 等权重
    for i, col in enumerate(cum_returns_equal.columns):
        col_name = f'Group {col}' if isinstance(col, int) else col
        fig.add_trace(
            go.Scatter(
                x=cum_returns_equal.index,
                y=cum_returns_equal[col],
                name=f'{col_name} (等权)',
                line=dict(color=colors[i % len(colors)], width=2),
                mode='lines',
                legendgroup=f'equal_{col}',
                visible=True
            ),
            row=1, col=1
        )

    if cum_long_short_equal is not None:
        fig.add_trace(
            go.Scatter(
                x=cum_long_short_equal.index,
                y=cum_long_short_equal,
                name='Long-Short (10-1) (等权)',
                line=dict(color='red', width=3),
                mode='lines',
                legendgroup='equal_ls',
                visible=True
            ),
            row=1, col=1
        )

    index_colors = {'沪深300': 'black', '上证50': 'purple', '中证1000': 'orange'}
    for idx_name, idx_returns in index_returns.items():
        idx_returns = idx_returns[idx_returns.index >= start]
        if len(idx_returns) > 0:
            idx_cum = (1 + idx_returns).cumprod()
            common_dates = cum_returns_equal.index.intersection(idx_cum.index)
            if len(common_dates) > 0:
                idx_cum_aligned = idx_cum.reindex(common_dates)
                fig.add_trace(
                    go.Scatter(
                        x=common_dates,
                        y=idx_cum_aligned,
                        name=idx_name,
                        line=dict(width=3, dash='dash', color=index_colors.get(idx_name, 'gray')),
                        mode='lines',
                        legendgroup=f'index_{idx_name}',
                        visible=True
                    ),
                    row=1, col=1
                )

    # 子图2: 市值加权
    for i, col in enumerate(cum_returns_mv.columns):
        col_name = f'Group {col}' if isinstance(col, int) else col
        fig.add_trace(
            go.Scatter(
                x=cum_returns_mv.index,
                y=cum_returns_mv[col],
                name=f'{col_name} (市值加权)',
                line=dict(color=colors[i % len(colors)], width=2),
                mode='lines',
                legendgroup=f'mv_{col}',
                visible=True
            ),
            row=2, col=1
        )

    if cum_long_short_mv is not None:
        fig.add_trace(
            go.Scatter(
                x=cum_long_short_mv.index,
                y=cum_long_short_mv,
                name='Long-Short (10-1) (市值加权)',
                line=dict(color='darkred', width=3),
                mode='lines',
                legendgroup='mv_ls',
                visible=True
            ),
            row=2, col=1
        )

    for idx_name, idx_returns in index_returns.items():
        idx_returns = idx_returns[idx_returns.index >= start]
        if len(idx_returns) > 0:
            idx_cum = (1 + idx_returns).cumprod()
            common_dates = cum_returns_mv.index.intersection(idx_cum.index)
            if len(common_dates) > 0:
                idx_cum_aligned = idx_cum.reindex(common_dates)
                fig.add_trace(
                    go.Scatter(
                        x=common_dates,
                        y=idx_cum_aligned,
                        name=f'{idx_name} (市值图)',
                        line=dict(width=3, dash='dash', color=index_colors.get(idx_name, 'gray')),
                        mode='lines',
                        legendgroup=f'index_mv_{idx_name}',
                        visible=True,
                        showlegend=False
                    ),
                    row=2, col=1
                )

    fig.add_hline(y=1, line_dash="solid", line_color="gray", line_width=1, opacity=0.5, row=1, col=1)
    fig.add_hline(y=1, line_dash="solid", line_color="gray", line_width=1, opacity=0.5, row=2, col=1)

    fig.update_xaxes(title_text="日期", row=2, col=1)
    fig.update_yaxes(title_text="累计收益", row=1, col=1)
    fig.update_yaxes(title_text="累计收益", row=2, col=1)

    fig.update_layout(
        height=1000,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.01
        ),
        hovermode='x unified'
    )

    output_file = output_dir / f'{factor_name.lower()}_cumulative_returns_combined.html'
    fig.write_html(str(output_file))
    logger.info(f"  合并累计收益曲线已保存: {output_file.name}")


def plot_combined_statistics(
    stats_equal: pd.DataFrame,
    stats_mv: pd.DataFrame,
    factor_name: str,
    output_dir: Path
):
    """
    绘制合并的统计指标对比图（等权 vs 市值加权）

    使用 Plotly 生成交互式 HTML 图表

    Args:
        stats_equal: 等权统计指标
        stats_mv: 市值加权统计指标
        factor_name: 因子名称
        output_dir: 输出目录
    """
    logger = logging.getLogger(__name__)
    logger.info("绘制合并统计指标对比图...")

    metrics_map = {
        'mean_return': '平均收益率(%)',
        'std_return': '收益率波动率(%)',
        'sharpe_ratio': '夏普比率',
        'cumulative_return': '累计收益率(%)'
    }

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[f'{factor_name} 因子 - {v}' for v in metrics_map.values()],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )

    positions = [(1, 1), (1, 2), (2, 1), (2, 2)]

    for (row, col), (metric_en, metric_cn) in zip(positions, metrics_map.items()):
        if metric_en not in stats_equal.columns or metric_en not in stats_mv.columns:
            continue

        fig.add_trace(
            go.Bar(
                name='等权重',
                x=stats_equal.index.astype(str),
                y=stats_equal[metric_en],
                text=[f'{v:.2f}' for v in stats_equal[metric_en]],
                textposition='outside',
                showlegend=(row == 1 and col == 1),
                legendgroup='equal',
                marker_color='lightblue'
            ),
            row=row, col=col
        )

        fig.add_trace(
            go.Bar(
                name='市值加权',
                x=stats_mv.index.astype(str),
                y=stats_mv[metric_en],
                text=[f'{v:.2f}' for v in stats_mv[metric_en]],
                textposition='outside',
                showlegend=(row == 1 and col == 1),
                legendgroup='mv',
                marker_color='lightcoral'
            ),
            row=row, col=col
        )

        fig.update_xaxes(title_text="分组", row=row, col=col)
        fig.update_yaxes(title_text=metric_cn, row=row, col=col)

    fig.update_layout(
        height=900,
        showlegend=True,
        barmode='group',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    output_file = output_dir / f'{factor_name.lower()}_statistics_combined.html'
    fig.write_html(str(output_file))
    logger.info(f"  合并统计指标图已保存: {output_file.name}")
