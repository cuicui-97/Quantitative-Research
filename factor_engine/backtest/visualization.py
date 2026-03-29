"""
可视化模块

负责生成因子分析的图表
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional

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
