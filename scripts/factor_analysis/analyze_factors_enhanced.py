#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子分析增强版

特性：
1. 测试时间：2015年至今
2. 合并市值加权和等权重展示
3. 添加指数基准对比（沪深300、上证50、中证1000）
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import logging
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from utils import setup_logger
from factor_engine import SingleFactorAnalyzer

# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False


def load_index_data():
    """
    加载指数数据

    Returns:
        dict: {指数名称: 收益率序列}
    """
    logger = logging.getLogger(__name__)
    logger.info("加载指数数据...")

    index_returns = {}
    index_codes = {
        '沪深300': '000300.SH',
        '上证50': '000016.SH',
        '中证1000': '000852.SH'
    }

    for name, code in index_codes.items():
        index_file = Config.SUPPLEMENTARY_DATA_DIR / f'{code}.csv'
        if index_file.exists():
            df = pd.read_csv(index_file, dtype={'trade_date': str})
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.set_index('trade_date').sort_index()

            # 计算开盘收益率
            if 'open' in df.columns:
                returns = df['open'].pct_change()
                index_returns[name] = returns
                logger.info(f"  {name}: {len(returns)} 个交易日")
            else:
                logger.warning(f"  {name}: 缺少开盘价数据")
        else:
            logger.warning(f"  {name}: 数据文件不存在 {index_file}")

    return index_returns


def plot_combined_returns(
    group_returns_equal: pd.DataFrame,
    group_returns_mv: pd.DataFrame,
    index_returns: dict,
    factor_name: str,
    output_dir: Path,
    start_date: str = '2015-01-01'
):
    """
    绘制合并的累计收益曲线（等权+市值加权+指数基准）- 使用 Plotly 生成可交互 HTML

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

    # 转换日期索引
    group_returns_equal.index = pd.to_datetime(group_returns_equal.index, format='%Y%m%d')
    group_returns_mv.index = pd.to_datetime(group_returns_mv.index, format='%Y%m%d')

    # 筛选日期范围
    start = pd.to_datetime(start_date)
    group_returns_equal = group_returns_equal[group_returns_equal.index >= start]
    group_returns_mv = group_returns_mv[group_returns_mv.index >= start]

    # 计算累计收益
    cum_returns_equal = (1 + group_returns_equal).cumprod()
    cum_returns_mv = (1 + group_returns_mv).cumprod()

    # 计算 Long-Short 组合收益（Group 10 - Group 1）
    # 列名可能是数字或字符串，都要检查
    if 10 in group_returns_equal.columns and 1 in group_returns_equal.columns:
        long_short_equal = group_returns_equal[10] - group_returns_equal[1]
        cum_long_short_equal = (1 + long_short_equal).cumprod()
        logger.info(f"等权 Long-Short 计算成功，累计收益最大值: {cum_long_short_equal.max():.2f}")
    elif 'Group 10' in group_returns_equal.columns and 'Group 1' in group_returns_equal.columns:
        long_short_equal = group_returns_equal['Group 10'] - group_returns_equal['Group 1']
        cum_long_short_equal = (1 + long_short_equal).cumprod()
        logger.info(f"等权 Long-Short 计算成功，累计收益最大值: {cum_long_short_equal.max():.2f}")
    else:
        cum_long_short_equal = None
        logger.warning(f"等权 Long-Short 计算失败，列名: {group_returns_equal.columns.tolist()}")

    if 10 in group_returns_mv.columns and 1 in group_returns_mv.columns:
        long_short_mv = group_returns_mv[10] - group_returns_mv[1]
        cum_long_short_mv = (1 + long_short_mv).cumprod()
        logger.info(f"市值加权 Long-Short 计算成功，累计收益最大值: {cum_long_short_mv.max():.2f}")
    elif 'Group 10' in group_returns_mv.columns and 'Group 1' in group_returns_mv.columns:
        long_short_mv = group_returns_mv['Group 10'] - group_returns_mv['Group 1']
        cum_long_short_mv = (1 + long_short_mv).cumprod()
        logger.info(f"市值加权 Long-Short 计算成功，累计收益最大值: {cum_long_short_mv.max():.2f}")
    else:
        cum_long_short_mv = None
        logger.warning(f"市值加权 Long-Short 计算失败，列名: {group_returns_mv.columns.tolist()}")

    # 创建子图
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(
            f'{factor_name} 因子分组累计收益曲线 - 等权重（{start_date}至今）',
            f'{factor_name} 因子分组累计收益曲线 - 市值加权（{start_date}至今）'
        ),
        vertical_spacing=0.1
    )

    # 颜色映射
    colors = ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#d9ef8b',
              '#a6d96a', '#66bd63', '#1a9850', '#006837', '#004529']

    # ========== 子图1: 等权重 ==========
    for i, col in enumerate(cum_returns_equal.columns):
        # 处理列名（可能是数字或字符串）
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

    # 添加 Long-Short 组合（等权）
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

    # 添加指数基准（等权图）
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

    # ========== 子图2: 市值加权 ==========
    for i, col in enumerate(cum_returns_mv.columns):
        # 处理列名（可能是数字或字符串）
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

    # 添加 Long-Short 组合（市值加权）
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

    # 添加指数基准（市值加权图）
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
                        showlegend=False  # 指数在第一个图中已经显示了，这里不重复
                    ),
                    row=2, col=1
                )

    # 添加基准线
    fig.add_hline(y=1, line_dash="solid", line_color="gray", line_width=1, opacity=0.5, row=1, col=1)
    fig.add_hline(y=1, line_dash="solid", line_color="gray", line_width=1, opacity=0.5, row=2, col=1)

    # 更新布局
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

    # 保存为 HTML
    output_file = output_dir / f'{factor_name.lower()}_cumulative_returns_combined.html'
    fig.write_html(str(output_file))

    logger.info(f"  合并累计收益曲线已保存: {output_file}")


def plot_combined_statistics(
    stats_equal: pd.DataFrame,
    stats_mv: pd.DataFrame,
    factor_name: str,
    output_dir: Path
):
    """
    绘制合并的统计指标对比图（等权 vs 市值加权）- 使用 Plotly 生成可交互 HTML

    Args:
        stats_equal: 等权统计指标
        stats_mv: 市值加权统计指标
        factor_name: 因子名称
        output_dir: 输出目录
    """
    logger = logging.getLogger(__name__)
    logger.info("绘制合并统计指标对比图...")

    # 指标映射：英文列名 -> 中文显示名
    metrics_map = {
        'mean_return': '平均收益率(%)',
        'std_return': '收益率波动率(%)',
        'sharpe_ratio': '夏普比率',
        'cumulative_return': '累计收益率(%)'
    }

    # 创建子图
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[f'{factor_name} 因子 - {v}' for v in metrics_map.values()],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )

    positions = [(1, 1), (1, 2), (2, 1), (2, 2)]

    for (row, col), (metric_en, metric_cn) in zip(positions, metrics_map.items()):
        if metric_en not in stats_equal.columns or metric_en not in stats_mv.columns:
            logger.warning(f"跳过指标 {metric_en}（数据中不存在）")
            continue

        # 等权重
        fig.add_trace(
            go.Bar(
                name='等权重',
                x=stats_equal.index.astype(str),
                y=stats_equal[metric_en],
                text=[f'{v:.2f}' for v in stats_equal[metric_en]],
                textposition='outside',
                showlegend=(row == 1 and col == 1),  # 只在第一个子图显示图例
                legendgroup='equal',
                marker_color='lightblue'
            ),
            row=row, col=col
        )

        # 市值加权
        fig.add_trace(
            go.Bar(
                name='市值加权',
                x=stats_mv.index.astype(str),
                y=stats_mv[metric_en],
                text=[f'{v:.2f}' for v in stats_mv[metric_en]],
                textposition='outside',
                showlegend=(row == 1 and col == 1),  # 只在第一个子图显示图例
                legendgroup='mv',
                marker_color='lightcoral'
            ),
            row=row, col=col
        )

        # 更新坐标轴
        fig.update_xaxes(title_text="分组", row=row, col=col)
        fig.update_yaxes(title_text=metric_cn, row=row, col=col)

    # 更新布局
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

    # 保存为 HTML
    output_file = output_dir / f'{factor_name.lower()}_statistics_combined.html'
    fig.write_html(str(output_file))

    logger.info(f"  合并统计指标图已保存: {output_file}")


def analyze_factor(
    factor_name: str,
    factor_matrix: pd.DataFrame,
    return_matrix: pd.DataFrame,
    tradability_matrix: pd.DataFrame,
    mv_matrix: pd.DataFrame,
    index_returns: dict,
    output_dir: Path,
    start_date: str = '2015-01-01'
):
    """
    分析单个因子

    Args:
        factor_name: 因子名称
        factor_matrix: 因子矩阵
        return_matrix: 收益率矩阵
        tradability_matrix: 可交易矩阵
        mv_matrix: 市值矩阵
        index_returns: 指数收益率
        output_dir: 输出目录
        start_date: 起始日期
    """
    logger = logging.getLogger(__name__)
    logger.info(f"\n{'='*60}")
    logger.info(f"分析 {factor_name} 因子（{start_date}至今）")
    logger.info(f"{'='*60}")

    # 筛选日期范围
    start = pd.to_datetime(start_date)
    date_mask = pd.to_datetime(factor_matrix.index, format='%Y%m%d') >= start

    factor_filtered = factor_matrix.loc[date_mask]
    return_filtered = return_matrix.loc[date_mask]
    tradable_filtered = tradability_matrix.loc[date_mask]
    mv_filtered = mv_matrix.loc[date_mask]

    logger.info(f"筛选后数据范围: {len(factor_filtered)} 个交易日, {len(factor_filtered.columns)} 只股票")

    # 创建分析器
    analyzer = SingleFactorAnalyzer(
        factor_name=factor_name,
        factor_matrix=factor_filtered,
        return_matrix=return_filtered,
        tradability_matrix=tradable_filtered,
        mv_matrix=mv_filtered,
        n_groups=10,
        logger=logger
    )

    # 运行分析
    results = analyzer.run_analysis(output_dir=output_dir, save_results=True)

    # 绘制增强版图表
    plot_combined_returns(
        group_returns_equal=results['group_returns_equal'],
        group_returns_mv=results['group_returns_mv'],
        index_returns=index_returns,
        factor_name=factor_name,
        output_dir=output_dir,
        start_date=start_date
    )

    plot_combined_statistics(
        stats_equal=results['stats_equal'],
        stats_mv=results['stats_mv'],
        factor_name=factor_name,
        output_dir=output_dir
    )

    logger.info(f"{factor_name} 因子分析完成")

    return results


def main():
    """主函数"""
    import logging
    logger = setup_logger()

    logger.info("="*60)
    logger.info("因子分析增强版（2015年至今，含指数基准对比）")
    logger.info("="*60)

    # 1. 加载矩阵数据
    logger.info("\n加载矩阵数据...")
    pb_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'pb_matrix.csv')
    mv_circ_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    logger.info(f"  PB矩阵: {pb_matrix.shape}")
    logger.info(f"  流通市值矩阵: {mv_circ_matrix.shape}")
    logger.info(f"  收益率矩阵: {return_matrix.shape}")
    logger.info(f"  可交易矩阵: {tradability_matrix.shape}")

    # 2. 对齐矩阵
    logger.info("\n对齐矩阵...")
    common_dates = (pb_matrix.index
                   .intersection(return_matrix.index)
                   .intersection(tradability_matrix.index)
                   .intersection(mv_circ_matrix.index))
    common_stocks = (pb_matrix.columns
                    .intersection(return_matrix.columns)
                    .intersection(tradability_matrix.columns)
                    .intersection(mv_circ_matrix.columns))

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    pb = pb_matrix.loc[common_dates, common_stocks]
    mv_circ = mv_circ_matrix.loc[common_dates, common_stocks]
    returns = return_matrix.loc[common_dates, common_stocks]
    tradable = tradability_matrix.loc[common_dates, common_stocks]

    # 3. 加载指数数据
    index_returns = load_index_data()

    # 4. 分析 PB 因子
    pb_output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'pb_factor'
    pb_output_dir.mkdir(parents=True, exist_ok=True)

    pb_results = analyze_factor(
        factor_name='PB',
        factor_matrix=pb,
        return_matrix=returns,
        tradability_matrix=tradable,
        mv_matrix=mv_circ,
        index_returns=index_returns,
        output_dir=pb_output_dir,
        start_date='2015-01-01'
    )

    # 5. 分析市值因子
    mv_output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'mv_factor'
    mv_output_dir.mkdir(parents=True, exist_ok=True)

    mv_results = analyze_factor(
        factor_name='MV',
        factor_matrix=mv_circ,
        return_matrix=returns,
        tradability_matrix=tradable,
        mv_matrix=mv_circ,
        index_returns=index_returns,
        output_dir=mv_output_dir,
        start_date='2015-01-01'
    )

    logger.info("\n" + "="*60)
    logger.info("所有因子分析完成")
    logger.info("="*60)
    logger.info(f"结果保存在: {Config.DATA_DIR / 'factor_analysis_results'}")


if __name__ == '__main__':
    main()
