#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
微盘股内流动性因子分析

测试因子：
1. Amihud_20d - Amihud非流动性（值越大流动性越差）
2. Turnover_20d - 20日平均换手率
3. Turnover_Vol_20d - 20日换手率波动率
4. VP_Corr_20d - 价量相关系数
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from utils import setup_logger
from factor_engine import SingleFactorAnalyzer
from factor_engine.backtest.data_loader import load_index_data
from factor_engine.backtest.visualization import plot_combined_returns, plot_combined_statistics


def run_microcap_factor_analysis(factor_name: str, factor_matrix_file: str, enable_cost: bool, logger):
    """在微盘股内运行单个因子分析"""
    cost_tag = 'with_cost' if enable_cost else 'no_cost'

    logger.info(f"\n{'='*60}")
    logger.info(f"{factor_name} 微盘股内因子分析（{'含成本' if enable_cost else '不含'}）")
    logger.info(f"{'='*60}")

    # 加载矩阵
    factor_matrix = load_matrix(Config.MATRIX_DATA_DIR / factor_matrix_file)
    mv_circ_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')
    microcap_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'microcap_matrix.csv')

    logger.info(f"  因子矩阵: {factor_matrix.shape}")
    logger.info(f"  微盘股矩阵: {microcap_matrix.shape}")

    # 对齐
    common_dates = (factor_matrix.index
                    .intersection(return_matrix.index)
                    .intersection(tradability_matrix.index)
                    .intersection(mv_circ_matrix.index)
                    .intersection(microcap_matrix.index))
    common_stocks = (factor_matrix.columns
                     .intersection(return_matrix.columns)
                     .intersection(tradability_matrix.columns)
                     .intersection(mv_circ_matrix.columns)
                     .intersection(microcap_matrix.columns))

    factor = factor_matrix.loc[common_dates, common_stocks]
    mv = mv_circ_matrix.loc[common_dates, common_stocks]
    returns = return_matrix.loc[common_dates, common_stocks]
    tradable = tradability_matrix.loc[common_dates, common_stocks]
    microcap = microcap_matrix.loc[common_dates, common_stocks]

    logger.info(f"  对齐后: {len(common_dates)} 个交易日, {len(common_stocks)} 只股票")

    # 合并可交易矩阵与微盘股矩阵
    microcap_tradable = ((tradable == 1) | (microcap == 0)).astype(np.int8)

    # 统计微盘股内可交易股票数量
    microcap_tradable_count = ((tradable == 0) & (microcap == 1)).sum().sum()
    total_cells = len(common_dates) * len(common_stocks)
    logger.info(f"  微盘股内可交易比例: {microcap_tradable_count / total_cells:.2%}")

    index_returns = load_index_data(Config.SUPPLEMENTARY_DATA_DIR)

    analyzer = SingleFactorAnalyzer(
        factor_name=factor_name,
        factor_matrix=factor,
        return_matrix=returns,
        tradability_matrix=microcap_tradable,
        mv_matrix=mv,
        n_groups=10,
        enable_transaction_cost=enable_cost,
        commission_rate=Config.COMMISSION_RATE,
        stamp_duty_rate=Config.STAMP_DUTY_RATE,
        slippage_rate=Config.SLIPPAGE_RATE,
        logger=logger
    )

    output_dir = Config.DATA_DIR / 'factor_analysis_results' / 'microcap' / f'{factor_name.lower()}_{cost_tag}'
    output_dir.mkdir(parents=True, exist_ok=True)

    results = analyzer.run_analysis(output_dir=output_dir, save_results=True)

    plot_combined_returns(
        group_returns_equal=results['group_returns_equal'],
        group_returns_mv=results['group_returns_mv'],
        index_returns=index_returns,
        factor_name=f'{factor_name} (MicroCap)',
        output_dir=output_dir,
        start_date='2015-01-01'
    )

    plot_combined_statistics(
        stats_equal=results['stats_equal'],
        stats_mv=results['stats_mv'],
        factor_name=f'{factor_name} (MicroCap)',
        output_dir=output_dir
    )

    # 输出核心指标
    stats = results['stats_equal']
    ls_stats = stats.loc['Long-Short (10-1)']
    logger.info(f"\n{factor_name} 微盘股 L-S 表现:")
    logger.info(f"  年化收益: {ls_stats['annual_return']:.2f}%")
    logger.info(f"  夏普比率: {ls_stats['sharpe_ratio']:.2f}")
    logger.info(f"  最大回撤: {ls_stats['max_drawdown']:.2f}%")
    logger.info(f"  IC均值: {results.get('ic_series', pd.Series()).mean():.4f}")

    return results


def main():
    logger = setup_logger(prefix="liquidity_microcap")

    logger.info("="*70)
    logger.info("微盘股内流动性因子分析")
    logger.info("="*70)

    # 定义要测试的流动性因子
    factors = [
        ('Amihud_20d', 'amihud_20d_matrix.csv'),
        ('Turnover_20d', 'turnover_20d_matrix.csv'),
        ('Turnover_Vol_20d', 'turnover_vol_20d_matrix.csv'),
        ('VP_Corr_20d', 'vp_corr_20d_matrix.csv'),
    ]

    summary = []

    for factor_name, matrix_file in factors:
        try:
            logger.info(f"\n\n{'#'*70}")
            logger.info(f"# 开始分析: {factor_name} (微盘股)")
            logger.info(f"{'#'*70}")

            # 不含成本
            result_no_cost = run_microcap_factor_analysis(factor_name, matrix_file, False, logger)
            stats_no_cost = result_no_cost['stats_equal'].loc['Long-Short (10-1)']

            # 含成本
            result_with_cost = run_microcap_factor_analysis(factor_name, matrix_file, True, logger)
            stats_with_cost = result_with_cost['stats_equal'].loc['Long-Short (10-1)']

            summary.append({
                'factor': factor_name,
                'ann_ret_no_cost': stats_no_cost['annual_return'],
                'sharpe_no_cost': stats_no_cost['sharpe_ratio'],
                'ann_ret_with_cost': stats_with_cost['annual_return'],
                'sharpe_with_cost': stats_with_cost['sharpe_ratio'],
            })

        except Exception as e:
            logger.error(f"分析 {factor_name} 失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue

    # 输出汇总
    logger.info(f"\n\n{'='*70}")
    logger.info("微盘股内流动性因子汇总对比")
    logger.info(f"{'='*70}")

    if summary:
        summary_df = pd.DataFrame(summary)
        summary_df = summary_df.sort_values('sharpe_no_cost', ascending=False)

        for _, row in summary_df.iterrows():
            logger.info(f"{row['factor']:20s}: 年化={row['ann_ret_no_cost']:6.2f}%, 夏普={row['sharpe_no_cost']:.2f}")

    logger.info("\n全部完成！")


if __name__ == '__main__':
    main()
